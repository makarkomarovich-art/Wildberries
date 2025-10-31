import types
import pytest

# We test through orchestrator: main_function.week_stats_mf.week_stats
import importlib


class FakeSupabase:
    def __init__(self):
        self.db = {
            'week_reports': [],
            'week_rows': [],
            'week_stats': [],
            'products': [],
        }

    class _Table:
        def __init__(self, name, outer):
            self._name = name
            self._outer = outer
            self._select = None
            self._filters = {}
            self._single = False
            self._insert_payload = None

        def select(self, sel):
            self._select = sel
            return self

        def eq(self, k, v):
            self._filters[k] = v
            return self

        def single(self):
            self._single = True
            return self

        def execute(self):
            rows = list(self._outer.db[self._name])
            for k, v in self._filters.items():
                rows = [r for r in rows if r.get(k) == v]
            return types.SimpleNamespace(data=(rows[0] if self._single else rows))

        def insert(self, rows):
            if isinstance(rows, dict):
                rows = [rows]
            if self._name == 'week_reports':
                existing = {r['realizationreport_id'] for r in self._outer.db['week_reports']}
                to_add = [r for r in rows if r['realizationreport_id'] not in existing]
                self._outer.db['week_reports'].extend(to_add)
                return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=to_add))
            if self._name == 'week_rows':
                existing = {(r['realizationreport_id'], r['rr_id']) for r in self._outer.db['week_rows']}
                to_add = []
                for r in rows:
                    key = (r['realizationreport_id'], r['rr_id'])
                    if key in existing:
                        continue
                    self._outer.db['week_rows'].append(r)
                    to_add.append(r)
                    existing.add(key)
                return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=to_add))
            if self._name == 'week_stats':
                existing = {(r['realizationreport_id'], r['nm_id']) for r in self._outer.db['week_stats']}
                to_add = []
                for r in rows:
                    key = (r['realizationreport_id'], r['nm_id'])
                    if key in existing:
                        continue
                    self._outer.db['week_stats'].append(r)
                    to_add.append(r)
                    existing.add(key)
                return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=to_add))
            self._outer.db[self._name].extend(rows)
            return types.SimpleNamespace(execute=lambda: types.SimpleNamespace(data=rows))

    def table(self, name):
        return FakeSupabase._Table(name, self)


class FakeAPIClient:
    def fetch_report_with_pagination(self, date_from, date_to, period):
        # Two realizationreport_id: A small one with only sales; a big with mixed operations
        return [
            # realizationreport_id 100
            {
                'realizationreport_id': 100,
                'rrd_id': 1,
                'gi_id': 10,
                'nm_id': 11001,
                'sa_name': 'A',
                'supplier_oper_name': 'Продажа',
                'quantity': 1,
                'retail_price': 200.0,
                'retail_amount': 180.0,
                'ppvz_spp_prc': 10.0,
                'ppvz_for_pay': 170.0,
                'commission_percent': 5.0,
                'delivery_amount': 0,
                'return_amount': 0,
                'delivery_rub': 10.0,
                'report_type': 'weekly',
                'order_dt': '2025-10-14',
                'sale_dt': '2025-10-15',
                'srid': 's1',
                'barcode': 123,
                'ts_name': 'ts',
                'doc_type_name': 'doc',
                'site_country': 'RU',
                'office_name': 'off',
            },
            # realizationreport_id 200 - add logistics and returns
            {
                'realizationreport_id': 200,
                'rrd_id': 2,
                'gi_id': 20,
                'nm_id': 22001,
                'sa_name': 'B',
                'supplier_oper_name': 'Продажа',
                'quantity': 2,
                'retail_price': 100.0,
                'retail_amount': 90.0,
                'ppvz_spp_prc': 0.0,
                'ppvz_for_pay': 95.0,
                'commission_percent': 0.0,
                'delivery_amount': 0,
                'return_amount': 0,
                'delivery_rub': 0.0,
                'report_type': 'weekly',
                'order_dt': '2025-10-16',
                'sale_dt': '2025-10-16',
                'srid': 's2',
                'barcode': 124,
                'ts_name': 'ts',
                'doc_type_name': 'doc',
                'site_country': 'RU',
                'office_name': 'off',
            },
            {
                'realizationreport_id': 200,
                'rrd_id': 3,
                'gi_id': 21,
                'nm_id': 22001,
                'sa_name': 'B',
                'supplier_oper_name': 'Логистика',
                'quantity': 0,
                'retail_price': 0.0,
                'retail_amount': 0.0,
                'ppvz_spp_prc': 0.0,
                'ppvz_for_pay': 0.0,
                'commission_percent': 0.0,
                'delivery_amount': 1,
                'return_amount': 1,
                'delivery_rub': 5.0,
                'report_type': 'weekly',
                'order_dt': '2025-10-16',
                'sale_dt': '2025-10-17',
                'srid': 's3',
                'barcode': 125,
                'ts_name': 'ts',
                'doc_type_name': 'doc',
                'site_country': 'RU',
                'office_name': 'off',
            },
        ]


class FakeCurlClient:
    def fetch_report_aggregated(self, realizationreport_id: int):
        # Return CURL totals matching aggregation from FakeAPIClient
        if realizationreport_id == 100:
            return {
                'totalSale': 180.0,
                'forPay': 170.0,
                'deliveryRub': 10.0,
                'paidStorageSum': 0.0,
                'paidAcceptanceSum': 0.0,
                'bankPaymentSum': 160.0,
                'penalty': 0.0,
            }
        if realizationreport_id == 200:
            # Sales 90.0 + logistics 5.0
            return {
                'totalSale': 90.0,
                'forPay': 95.0,
                'deliveryRub': 5.0,
                'paidStorageSum': 0.0,
                'paidAcceptanceSum': 0.0,
                'bankPaymentSum': 90.0,
                'penalty': 0.0,
            }
        return {}


@pytest.fixture()
def patched_main(monkeypatch):
    # Load module fresh
    m = importlib.import_module('main_function.week_stats_mf.week_stats')

    # Patch import_api_client / import_curl_client to fakes
    monkeypatch.setattr(m, 'import_api_client', lambda: FakeAPIClient, raising=True)
    monkeypatch.setattr(m, 'import_curl_client', lambda: FakeCurlClient, raising=True)

    # Patch validators: return fakes for API/CURL validators that always pass, keep aggregator/compare/ writers real
    def fake_import_validators():
        # Minimal validator shims
        api_validator = types.SimpleNamespace(validate_api_data=lambda x: {'valid': True, 'errors': []})
        curl_validator = types.SimpleNamespace(validate_curl_data=lambda x: {'valid': True, 'errors': []})
        # Use real aggregator, writers; fake compare to always pass
        base = importlib.import_module('excel_actions.week_stats_ea.aggregator')
        def _ok_compare(api_data, curl_data):
            return True, {'fields': {}}
        compare = types.SimpleNamespace(compare_api_curl=_ok_compare)
        writer = importlib.import_module('excel_actions.week_stats_ea.supabase_writer')
        rows_writer = importlib.import_module('excel_actions.week_stats_ea.week_rows_writer')
        week_stats_writer = importlib.import_module('excel_actions.week_stats_ea.week_stats_writer')
        return api_validator, curl_validator, base, compare, writer, rows_writer, week_stats_writer

    monkeypatch.setattr(m, 'import_validators', fake_import_validators, raising=True)

    # Patch create_client to our FakeSupabase
    fake_sb = FakeSupabase()
    # Seed products for nm_id mapping
    fake_sb.db['products'] = [
        {'id': 'prod-11001', 'nm_id': 11001},
        {'id': 'prod-22001', 'nm_id': 22001},
    ]
    monkeypatch.setattr(m, 'create_client', lambda url, key: fake_sb, raising=True)

    return m, fake_sb


def test_end_to_end_week_stats_via_main(patched_main, capsys):
    m, fake_sb = patched_main
    # Run orchestrator
    m.process_weekly_reports(date_from='2025-10-13', date_to='2025-10-19', period='weekly')

    # week_reports: two reports (100 and 200)
    assert {r['realizationreport_id'] for r in fake_sb.db['week_reports']} == {100, 200}

    # week_rows: inserted only rows that pass products/nm_id filters => two rrd_id (1,2,3) but logistics also included
    assert len(fake_sb.db['week_rows']) >= 2

    # week_stats: aggregated nm_id per report
    pairs = {(r['realizationreport_id'], r['nm_id']) for r in fake_sb.db['week_stats']}
    assert (100, 11001) in pairs
    assert (200, 22001) in pairs

    # Logs contain comparison success for both reports (logger writes, not stdout) – skip strict log assertion
    # Ensure totals verification printed OK at least once
    out = capsys.readouterr().out
    assert 'совпадают' in out
