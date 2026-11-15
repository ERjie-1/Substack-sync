"""Extract only pure translation definitions: no app import, env, credentials or network."""
import ast
import contextlib
import hashlib
import io
import json
import re
import types
import subprocess
import unittest
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import Mock

ROOT=Path(__file__).resolve().parents[1]
NAMES={'TranslationError','TranslationResponse','_translation_label_diagnostics',
       '_deepseek_batch_diagnostics','_safe_deepseek_error','call_deepseek_api',
       'parse_translation_response','translate_blocks_deepseek'}
def load(source=None):
    tree=ast.parse(source if source is not None else (ROOT/'sync_substack.py').read_text())
    ns=dict(List=List,Dict=Dict,Optional=Optional,re=re,hashlib=hashlib,json=json,
            DEEPSEEK_API_KEY='synthetic-fixture',DEEPSEEK_MODEL='deepseek-chat',
            DEEPSEEK_BASE_URL='https://example.invalid',TRANSLATION_SYSTEM_PROMPT='fixture',
            ENABLE_TRANSLATION=True,MAX_CHARS_PER_BATCH=6000,MAX_BLOCKS_PER_BATCH=80,
            time=types.SimpleNamespace(sleep=Mock()),should_translate_block=lambda b:(True,'fixture'))
    class Timeout(Exception):pass
    ns['requests']=types.SimpleNamespace(post=Mock(),exceptions=types.SimpleNamespace(Timeout=Timeout))
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))and n.name in NAMES]
    if source is None: assert {n.name for n in nodes}==NAMES
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(ROOT/'sync_substack.py'),'exec'),ns)
    return ns

def response(text,reason='length'):
    return types.SimpleNamespace(status_code=200,text='synthetic-json',json=lambda:{'choices':[{'message':{'content':text},'finish_reason':reason}]})
def labels(indices):return '\n'.join(f'[P{i}] synthetic translated text' for i in indices)
def blocks():return [{'type':'paragraph','paragraph':{'rich_text':[{'text':{'content':('x'*250 if i<24 else 'x'*912)}}]}}for i in range(25)]

class DiagnosticsTests(unittest.TestCase):
    def setUp(self):self.n=load()
    def run_batch(self,text,reason='length',source=None):
        self.n['requests'].post.return_value=response(text,reason)
        with contextlib.redirect_stdout(io.StringIO())as output:
            try:return self.n['translate_blocks_deepseek'](blocks() if source is None else source),None,output.getvalue()
            except self.n['TranslationError']as error:return None,error,output.getvalue()
    def test_missing_15_of_25(self):
        result,error,log=self.run_batch(labels(range(1,16)))
        self.assertIsNone(result);d=error.diagnostics
        self.assertEqual(d['translated_count'],15);self.assertEqual(d['expected_count'],25)
        self.assertEqual(d['missing_chunk_labels'],list(range(16,26)))
        self.assertEqual(d['duplicate_chunk_labels'],[]);self.assertEqual(d['finish_reason'],'length')
        self.assertEqual(self.n['requests'].post.call_count,1)
        self.assertNotIn('synthetic translated text',log)
    def test_duplicate_labels_same_15_of_25(self):
        _,error,_=self.run_batch(labels(list(range(1,16))+list(range(1,11))),'stop')
        self.assertEqual(error.diagnostics['translated_count'],15)
        self.assertEqual(error.diagnostics['duplicate_chunk_labels'],list(range(1,11)))
        self.assertEqual(error.diagnostics['duplicate_label_occurrences'],10)
        self.assertEqual(error.diagnostics['finish_reason'],'stop')
    def test_success_payload_and_request_contract(self):
        source=blocks();result,error,_=self.run_batch(labels(range(1,26)),'stop',source)
        self.assertIsNone(error);self.assertEqual(len(result),25)
        self.assertEqual(source,blocks())
        for r in result:self.assertEqual(r['paragraph']['rich_text'][-1]['text']['content'],'synthetic translated text')
        request=self.n['requests'].post.call_args
        self.assertEqual(request.kwargs['timeout'],60)
        self.assertEqual(request.kwargs['json']['max_tokens'],8000)
        self.assertEqual(request.kwargs['json']['temperature'],0.3)
        self.assertFalse(request.kwargs['json']['stream'])
        self.assertEqual(request.kwargs['json']['messages'][0]['content'],'fixture')
        self.assertEqual(len(request.kwargs['json']['messages'][1]['content'].splitlines()),25)
    def test_unknown_reason_not_persisted(self):
        _,error,log=self.run_batch(labels(range(1,16)),'private-secret-text')
        self.assertEqual(error.diagnostics['finish_reason'],'unknown')
        self.assertNotIn('private-secret-text',log)
    def test_missing_or_nonstring_reason(self):
        for reason in (None,{},12):
            with self.subTest(reason=reason):
                _,error,_=self.run_batch(labels(range(1,16)),reason)
                self.assertEqual(error.diagnostics['finish_reason'],'unknown')
    def test_plain_string_compatibility(self):
        r=self.n['TranslationResponse']('abc','stop');self.assertIsInstance(r,str)
        self.assertEqual(r,'abc');self.assertEqual(json.dumps(r),'"abc"')
        d=self.n['_translation_label_diagnostics'](labels([1,1,2]),['last','ok',None])
        self.assertEqual(d['finish_reason'],'unknown');self.assertEqual(d['missing_chunk_labels'],[3])
    def test_out_of_range_labels_not_retained(self):
        d=self.n['_translation_label_diagnostics'](labels([0,999999,1,1]),['last',None])
        self.assertEqual(d['duplicate_chunk_labels'],[1]);self.assertNotIn('999999',str(d))
    def test_original_empty_translation_gate_unchanged(self):
        # Empty strings are not changed into a new rejection criterion.
        p=self.n['parse_translation_response']('[P1] ',1)
        self.assertEqual(p,[''])
    def test_frozen_baseline_payloads_requests_and_gate_match(self):
        baseline=subprocess.check_output(['git','show','f51e1377c2ccea84714a6b85cf0c841fedfbc1f1:sync_substack.py'],cwd=ROOT,text=True)
        for text in (labels(range(1,16)),labels(list(range(1,16))+list(range(1,11))),labels(range(1,26))):
            outcomes=[];calls=[]
            for ns in (load(baseline),load()):
                ns['requests'].post.return_value=response(text)
                with contextlib.redirect_stdout(io.StringIO()):
                    try:outcomes.append(('success',ns['translate_blocks_deepseek'](blocks())))
                    except ns['TranslationError']as error:
                        d={k:v for k,v in error.diagnostics.items()if k not in {'finish_reason','missing_chunk_labels','duplicate_chunk_labels','duplicate_label_occurrences'}}
                        outcomes.append(('error',str(error),d))
                calls.append(ns['requests'].post.call_args_list)
            self.assertEqual(outcomes[0],outcomes[1]);self.assertEqual(calls[0],calls[1])

    def test_api_http_failure_still_single_request(self):
        self.n['requests'].post.return_value=types.SimpleNamespace(status_code=500,text='fixture',json=lambda:{})
        with contextlib.redirect_stdout(io.StringIO()),self.assertRaises(self.n['TranslationError']):self.n['call_deepseek_api'](['fixture'])
        self.assertEqual(self.n['requests'].post.call_count,1)

if __name__=='__main__':unittest.main()
