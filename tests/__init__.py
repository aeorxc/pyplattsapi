from plattsapi import plattsapi as platts_api

def test_get_crude_supply_by_country():
    res = platts_api.getMonthlyCrudeProductionByCountry('Saudi Arabia', 2021)
    assert len(res) == 12
    assert 'sum_value' in res.columns

def test_get_capacity_changes_by_country():
    res = platts_api.getCountryCapacityChangesTimeSeries('China')
    assert 'Mbdc' in res.columns
    assert 'Mmtcd' in res.columns
    assert 'Mmtcy' in res.columns

def test_get_yearly_runs_by_country():
    res = platts_api.getCountryYearlyRunsTimeSeries('China')
    assert 'Mbdc' in res.columns
    assert 'Mmtcd' in res.columns
    assert 'Mmtcy' in res.columns

def test_get_yearly_margins_by_type():
    res = platts_api.getMarginsbyType('Dated Brent NWE Cracking')
    assert 'Date' in res.columns
    assert 'Margin' in res.columns

def test_us_inventories_by_product():
    res = platts_api.getUSInventoriesByProduct('Central Atlantic Distillate')
    assert 'Date' in res.columns
    assert 'Volume' in res.columns

def test_us_imports_by_product():
    res = platts_api.getUSImportsByProduct('Fuel Oil')
    assert 'ArrivalDate' in res.columns
    assert 'Volume' in res.columns

def test_us_crude_exports():
    res = platts_api.getUSCrudeExports()
    assert 'EIAWeekEnding' in res.columns
    assert 'ForecastType' in res.columns
    assert 'Volume_MBD' in res.columns