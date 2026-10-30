from pathlib import Path
import tempfile,subprocess,os,json,hashlib,argparse
parser=argparse.ArgumentParser();parser.add_argument('--binary',type=Path,default=Path('/work/cargo-target-ci-1.95/debug/examples/backend_qos_cleanup'));binary=parser.parse_args().binary.resolve();checks=[]
with tempfile.TemporaryDirectory(prefix='qos-cleanup-review-') as temp:
 root=Path(temp);shared=root/'shared';shared.mkdir();sentinel=shared/'sentinel';sentinel.write_bytes(b'keep')
 run=root/'nightly-test';run.mkdir();(run/'destination').symlink_to(shared,target_is_directory=True)
 linkroot=root/'release-test';linkroot.symlink_to(shared,target_is_directory=True)
 fake=root/'x:'/'host';fake.mkdir(parents=True);(fake/'nightly-test').symlink_to(shared,target_is_directory=True)
 cases=[('parent',str(run/'..'/'shared'),'nightly-test'),('multiple-parents',str(run/'..'/'..'),'nightly-test'),('run-symlink',str(linkroot),'release-test'),('child-symlink',str(run/'destination'),'nightly-test'),('encoded-parent','s3://user:pass@bucket.host/ci/nightly-test/%2e%2e/shared','nightly-test'),('credential-slash','s3://user:pass/nightly-test/x@bucket.host/shared','nightly-test'),('query-is-not-root','s3://user:pass@bucket.host?prefix=/nightly-test/','nightly-test'),('unknown-file-scheme','file://host/nightly-test/link','nightly-test'),('unknown-uppercase-scheme','NFS://host/nightly-test/link','nightly-test'),('unknown-local-scheme-symlink',str(root)+'/x://host/nightly-test','nightly-test')]
 for name,endpoint,run_id in cases:
  v=subprocess.run([str(binary),'--run-id',run_id,'--tree'],env={**os.environ,'QOS_SOURCE':endpoint},capture_output=True,text=True,timeout=20)
  assert v.returncode!=0,(name,v.stdout,v.stderr);assert sentinel.read_bytes()==b'keep',name
  checks.append({'case':name,'rejected':True,'sibling_sentinel_preserved':True})
 valid=root/'nightly-valid';(valid/'nested').mkdir(parents=True);(valid/'nested'/'payload').write_bytes(b'owned')
 v=subprocess.run([str(binary),'--run-id','nightly-valid','--tree'],env={**os.environ,'QOS_SOURCE':str(valid)},capture_output=True,text=True,timeout=20)
 assert v.returncode==0,(v.stdout,v.stderr);assert valid.is_dir() and not list(valid.iterdir());assert sentinel.read_bytes()==b'keep'
 checks.append({'case':'valid-local-tree','deleted_owned_children_only':True,'root_retained':True,'sibling_sentinel_preserved':True})
result={'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'checks':checks,'passed':len(checks),'failed':0,'scope':'actual cleanup executable; only disposable local trees; no external backend contacted'}
Path('/work/qos-review-20261030/cleanup-safety-e2e.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
