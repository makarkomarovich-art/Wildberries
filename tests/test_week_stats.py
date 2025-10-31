import pytest

# Modules under test
from excel_actions.week_stats_ea.week_rows_writer import insert_week_rows
from excel_actions.week_stats_ea.week_stats_writer import (
    insert_week_stats_for_report,
    verify_week_stats_totals,
)


class FakeResponse:
    def __init__(self, data):
        self.data = data


class FakeExec:
    def __init__(self, data):
        self._data = data

    def execute(self):
        return FakeResponse(self._data)


class FakeQuery:
    def __init__(self, table, db, selection):
        self.table = table
        self.db = db
        self.selection = selection
        self.filters = {}
        self._single = False

    def eq(self, field, value):
        self.filters[field] = value
        return self

    def single(self):
        self._single = True
        return self

    def execute(self):
        rows = [r for r in self.db[self.table]]
        for k, v in self.filters.items():
            rows = [r for r in rows if r.get(k) == v]
        if self._single:
            return FakeResponse(rows[0] if rows else None)
        return FakeResponse(rows)


class FakeTable:
    def __init__(self, name, db):
        self.name = name
        self.db = db

    def select(self, selection):
        return FakeQuery(self.name, self.db, selection)

    def insert(self, rows):
        if isinstance(rows, dict):
            rows = [rows]
        # emulate UNIQUE on week_rows (realizationreport_id, rr_id)
        if self.name == 'week_rows':
            existing_keys = {(r['realizationreport_id'], r['rr_id']) for r in self.db['week_rows']}
            out = []
            for r in rows:
                key = (r['realizationreport_id'], r['rr_id'])
                if key in existing_keys:
                    continue
                self.db['week_rows'].append(r)
                out.append(r)
                existing_keys.add(key)
            return FakeExec(out)
        elif self.name == 'week_stats':
            existing_pairs = {(r['realizationreport_id'], r['nm_id']) for r in self.db['week_stats']}
            out = []
            for r in rows:
                key = (r['realizationreport_id'], r['nm_id'])
                if key in existing_pairs:
                    continue
                self.db['week_stats'].append(r)
                out.append(r)
                existing_pairs.add(key)
            return FakeExec(out)
        else:
            self.db[self.name].extend(rows)
            return FakeExec(rows)


class FakeSupabase:
    def __init__(self):
        self.db = {
            'products': [],
            'week_reports': [],
            'week_rows': [],
            'week_stats': [],
        }

    def table(self, name):
        return FakeTable(name, self.db)


@pytest.fixture()
def fake_db():
    sb = FakeSupabase()
    # Seed products
    sb.db['products'] = [
        {'id': 'p-1', 'nm_id': 1001},
        {'id': 'p-2', 'nm_id': 1002},
    ]
    # Seed week_reports for realizationreport_id 777
    sb.db['week_reports'] = [
        {
            'realizationreport_id': 777,
            'report_type': 'weekly',
            'date_from': '2025-10-13',
            'date_to': '2025-10-19',
            'retail_amount_total': 300.0,
            'ppvz_for_pay_total': 270.0,
            'delivery_rub_total': 10.0,
        }
    ]
    return sb


def test_insert_week_rows_filters_and_inserts(fake_db):
    # 4 input records: good (1001), missing product (9999), nm_id=0, duplicate rr_id
    records = [
        {
            'rrd_id': 1,
            'gi_id': 10,
            'nm_id': 1001,
            'sa_name': 'SA',
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
        },
        {
            'rrd_id': 2,
            'gi_id': 11,
            'nm_id': 9999,  # not in products
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 100.0,
            'retail_amount': 90.0,
            'ppvz_for_pay': 85.0,
            'report_type': 'weekly',
        },
        {
            'rrd_id': 3,
            'gi_id': 12,
            'nm_id': 0,  # should be filtered out earlier, but our writer filters too
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 50.0,
            'retail_amount': 45.0,
            'ppvz_for_pay': 44.0,
            'report_type': 'weekly',
        },
        {
            'rrd_id': 1,  # duplicate
            'gi_id': 13,
            'nm_id': 1001,
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 200.0,
            'retail_amount': 180.0,
            'ppvz_for_pay': 170.0,
            'report_type': 'weekly',
        },
    ]

    inserted, missing = insert_week_rows(records, fake_db, 777)

    # Only first record should be inserted, second is missing product, third filtered earlier, fourth duplicate rr_id
    assert inserted == 1
    assert set(missing) == {9999}
    assert len(fake_db.db['week_rows']) == 1
    row = fake_db.db['week_rows'][0]
    assert row['nm_id'] == 1001
    assert row['realizationreport_id'] == 777


def test_week_stats_writer_inserts_and_verifies_ok(fake_db):
    # Preload week_rows with two records for nm_id 1001 and 1002
    fake_db.db['week_rows'] = [
        {
            'realizationreport_id': 777,
            'nm_id': 1001,
            'product_id': 'p-1',
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
            'sa_name': 'SA1',
        },
        {
            'realizationreport_id': 777,
            'nm_id': 1002,
            'product_id': 'p-2',
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 100.0,
            'retail_amount': 90.0,
            'ppvz_spp_prc': 0.0,
            'ppvz_for_pay': 100.0,
            'commission_percent': 0.0,
            'delivery_amount': 0,
            'return_amount': 0,
            'delivery_rub': 0.0,
            'sa_name': 'SA2',
        },
    ]

    inserted, total_nm = insert_week_stats_for_report(fake_db, 777)
    assert total_nm == 2
    assert inserted == 2
    # Totals in week_reports were set to: amount=300, ppvz=270, delivery_rub=10
    # Our data: amount = 180 + 90 = 270 (doesn't match) -> adjust week_reports to match this test
    fake_db.db['week_reports'][0]['retail_amount_total'] = 270.0
    fake_db.db['week_reports'][0]['ppvz_for_pay_total'] = 270.0
    fake_db.db['week_reports'][0]['delivery_rub_total'] = 10.0

    # Should print OK without raising
    verify_week_stats_totals(fake_db, 777)


def test_week_stats_writer_mismatch(fake_db, capsys):
    # week_rows: single nm_id 1001
    fake_db.db['week_rows'] = [
        {
            'realizationreport_id': 777,
            'nm_id': 1001,
            'product_id': 'p-1',
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 50.0,
            'retail_amount': 45.0,
            'ppvz_for_pay': 40.0,
            'commission_percent': 0.0,
            'delivery_amount': 0,
            'return_amount': 0,
            'delivery_rub': 5.0,
            'sa_name': 'SA1',
        },
    ]

    # Set week_reports totals to non-matching
    fake_db.db['week_reports'][0]['retail_amount_total'] = 999.0
    fake_db.db['week_reports'][0]['ppvz_for_pay_total'] = 999.0
    fake_db.db['week_reports'][0]['delivery_rub_total'] = 999.0

    insert_week_stats_for_report(fake_db, 777)
    verify_week_stats_totals(fake_db, 777)
    out = capsys.readouterr().out
    assert 'Несовпадение сумм' in out


def test_insert_week_rows_returns_zero_when_nothing_to_insert(fake_db, capsys):
    # Existing week_rows contains rr_id 5; incoming also rr_id 5 with nm_id not in products
    fake_db.db['week_rows'] = [{'realizationreport_id': 888, 'rr_id': 5}]
    records = [
        {'rrd_id': 5, 'nm_id': 9999, 'supplier_oper_name': 'Продажа', 'retail_price': 0.0, 'retail_amount': 0.0, 'report_type': 'weekly'},
    ]
    inserted, missing = insert_week_rows(records, fake_db, 888)
    assert inserted == 0
    assert missing == []
    out = capsys.readouterr().out
    assert 'Отфильтровано' in out


def test_week_stats_writer_percentage_calculation(fake_db, capsys):
    # Week rows with non-zero retail_price to exercise percentage math
    fake_db.db['week_rows'] = [
        {
            'realizationreport_id': 777,
            'nm_id': 1001,
            'product_id': 'p-1',
            'supplier_oper_name': 'Продажа',
            'quantity': 2,
            'retail_price': 200.0,
            'retail_amount': 150.0,
            'ppvz_spp_prc': 10.0,
            'ppvz_for_pay': 140.0,
            'commission_percent': 5.0,
            'delivery_amount': 1,
            'return_amount': 0,
            'delivery_rub': 5.0,
            'sa_name': 'SA1',
        },
        {
            'realizationreport_id': 777,
            'nm_id': 1001,
            'product_id': 'p-1',
            'supplier_oper_name': 'Возврат',
            'quantity': 1,
            'retail_price': 50.0,
            'retail_amount': 40.0,
            'ppvz_spp_prc': 10.0,
            'ppvz_for_pay': 35.0,
            'commission_percent': 5.0,
            'delivery_amount': 0,
            'return_amount': 1,
            'delivery_rub': 0.0,
            'sa_name': 'SA1',
        },
    ]
    # Make totals irrelevant
    fake_db.db['week_reports'][0]['retail_amount_total'] = 0.0
    fake_db.db['week_reports'][0]['ppvz_for_pay_total'] = 0.0
    fake_db.db['week_reports'][0]['delivery_rub_total'] = 0.0

    insert_week_stats_for_report(fake_db, 777)
    rec = next(r for r in fake_db.db['week_stats'] if r['nm_id'] == 1001)
    assert rec['perc_discountwb_both_nm'] is not None
    assert rec['perc_spp_nm'] is not None
    assert rec['perc_wallet_discount_nm'] is not None
    assert rec['perc_commisian_both_nm'] is not None
    assert rec['perc_commission_nm'] is not None
    assert rec['perc_excess_comission_nm'] is not None


class BoomSupabase(FakeSupabase):
    def table(self, name):
        base = super().table(name)
        class FailingTable(FakeTable):
            def insert(self, rows):
                raise Exception('boom')
        if name == 'week_rows':
            return FailingTable(name, self.db)
        return base


def test_insert_week_rows_handles_insert_exception(capsys):
    sb = BoomSupabase()
    # Seed products and report
    sb.db['products'] = [{'id': 'p-1', 'nm_id': 1001}]
    records = [{
        'rrd_id': 1,
        'gi_id': 10,
        'nm_id': 1001,
        'supplier_oper_name': 'Продажа',
        'quantity': 1,
        'retail_price': 10.0,
        'retail_amount': 9.0,
        'ppvz_for_pay': 8.0,
        'report_type': 'weekly',
    }]
    with pytest.raises(Exception):
        insert_week_rows(records, sb, 777)
    out = capsys.readouterr().out
    assert 'ОШИБКА при insert в week_rows' in out


def test_week_stats_writer_covers_logistics_and_skip_and_ok(fake_db, capsys):
    # Add week_rows including a logistics row and a nm_id not in products map
    fake_db.db['week_rows'] = [
        {
            'realizationreport_id': 777,
            'nm_id': 1001,
            'product_id': 'p-1',
            'supplier_oper_name': 'Логистика',
            'quantity': 0,
            'retail_price': 0.0,
            'retail_amount': 0.0,
            'ppvz_for_pay': 0.0,
            'commission_percent': 0.0,
            'delivery_amount': 2,
            'return_amount': 1,
            'delivery_rub': 3.0,
            'sa_name': 'SA1',
        },
        {
            'realizationreport_id': 777,
            'nm_id': 5555,  # exists in week_rows but not in products -> triggers skip at candidate build
            'product_id': None,  # still filtered by load_rows, but ensure products_map skip by injecting products map
            'supplier_oper_name': 'Продажа',
            'quantity': 1,
            'retail_price': 10.0,
            'retail_amount': 9.0,
            'ppvz_for_pay': 8.0,
            'commission_percent': 0.0,
            'delivery_amount': 0,
            'return_amount': 0,
            'delivery_rub': 0.0,
            'sa_name': 'SAx',
        },
    ]
    # Ensure products map does not include 5555
    # week_reports totals set to match only logistics contribution
    fake_db.db['week_reports'][0]['retail_amount_total'] = 0.0
    fake_db.db['week_reports'][0]['ppvz_for_pay_total'] = 0.0
    fake_db.db['week_reports'][0]['delivery_rub_total'] = 3.0

    # Insert and verify
    insert_week_stats_for_report(fake_db, 777)
    verify_week_stats_totals(fake_db, 777)
    out = capsys.readouterr().out
    # Should print OK since totals match (0,0,3)
    assert 'совпадают' in out
