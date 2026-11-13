#!/usr/bin/env python3
"""Portable control runner; build release api and perf/event-load first.
Retains raw evidence and SQLite databases in a new temporary directory.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    if not 2 <= args.seconds <= 600 or not 1 <= args.rounds <= 10:
        parser.error('seconds must be 2..600 and rounds 1..10')
    root = Path(__file__).resolve().parent.parent
    suffix = '.exe' if os.name == 'nt' else ''
    api = root / 'target/release' / ('api' + suffix)
    load = root / 'perf/target/release' / ('event-load' + suffix)
    evidence_root = root / 'target/soak-evidence'
    evidence_root.mkdir(parents=True, exist_ok=True)
    evidence = Path(tempfile.mkdtemp(prefix='run-', dir=evidence_root))
    filesystem = 'unverified; inspect the volume containing the evidence directory'
    if platform.system() == 'Linux':
        try:
            filesystem = subprocess.check_output(['findmnt', '-T', str(evidence), '-no', 'FSTYPE,TARGET'], text=True).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    report = {'platform': platform.platform(), 'evidence': str(evidence),
              'storage_filesystem': filesystem,
              'scope': 'local loopback, same-host closed-loop client, fresh WAL/FULL DB per case',
              'api_sha256': hashlib.sha256(api.read_bytes()).hexdigest(),
              'client_sha256': hashlib.sha256(load.read_bytes()).hexdigest(), 'results': []}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    for round_index in range(args.rounds):
        for concurrency in (8, 16):
            case = evidence / f'round-{round_index + 1}-c{concurrency}'
            case.mkdir()
            env = {k: v for k, v in os.environ.items() if not k.startswith('RECONCILE_')}
            with socket.socket() as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]
            base = f'http://127.0.0.1:{port}'
            db = case / 'events.sqlite3'
            key = secrets.token_hex(32)
            env.update(RECONCILE_API_KEY=key, RECONCILE_BIND=f'127.0.0.1:{port}',
                       RECONCILE_DB_PATH=str(db), RECONCILE_REQUESTS_PER_SECOND='10000',
                       RECONCILE_EVENT_WAIT_MS='60000')

            def get(path):
                req = urllib.request.Request(base + path, headers={'Authorization': 'Bearer ' + key})
                with urllib.request.urlopen(req, timeout=10) as response:
                    return response.read().decode()

            def start(log):
                proc = subprocess.Popen([str(api)], env=env, stdout=log, stderr=log)
                for _ in range(100):
                    if proc.poll() is not None:
                        raise RuntimeError('API exited during startup')
                    try:
                        get('/health')
                        return proc
                    except (OSError, TimeoutError):
                        time.sleep(.1)
                proc.kill()
                proc.wait()
                raise RuntimeError('Startup deadline exceeded')

            result = {'round': round_index + 1, 'concurrency': concurrency, 'validation_passed': False}
            server = None
            print(f'Round {round_index + 1}, concurrency {concurrency}', flush=True)
            try:
                with (case / 'server.log').open('wb') as log:
                    server = start(log)
                    run = subprocess.run([str(load), base, str(concurrency), str(args.seconds)],
                                         env=env, capture_output=True, timeout=args.seconds + 120)
                    (case / 'client.json').write_bytes(run.stdout)
                    (case / 'client.log').write_bytes(run.stderr)
                    result.update(json.loads(run.stdout))
                    result['operational_metrics'] = get('/metrics')
                    server.kill()
                    server.wait()
                    audit = subprocess.run([str(load), '--audit', str(db), str(result['completed_payments']), result['run_id']],
                                           capture_output=True, timeout=30)
                    result['database_audit'] = json.loads(audit.stdout)
                    server = start(log)
                    value = json.loads(get('/v1/payments/' + result['sample_payment_id']))
                    result['restart_lookup_passed'] = value['status'] == 'Matched' and value['provider']['version'] == 2
                    result['validation_passed'] = (run.returncode == 0 and audit.returncode == 0
                        and result['correctness_passed'] and result['database_audit']['passed'] and result['restart_lookup_passed'])
                    # On Linux this also exercises the real SIGTERM shutdown handler.
                    server.terminate()
                    server.wait(timeout=15)
                    result['shutdown_exit_code'] = server.returncode
                    if os.name != 'nt':
                        result['validation_passed'] &= server.returncode == 0
            except Exception as error:
                result['runner_error'] = str(error)
                result['validation_passed'] = False
            finally:
                if server is not None and server.poll() is None:
                    server.kill()
                    server.wait()
            report['results'].append(result)
            report['all_cases_passed'] = all(r['validation_passed'] for r in report['results'])
            args.report.write_text(json.dumps(report, indent=2) + '\n')
            print(f"Validation: {result['validation_passed']}", flush=True)
    return 0 if report['all_cases_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
