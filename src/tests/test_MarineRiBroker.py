from typing import Dict, List

import pytest
from pykg2tbl import DefaultSparqlBuilder

from marine_eov_broker.MarineRiBroker import MarineBroker
from marine_eov_broker.NVSQueries import DEFAULT_TEMPLATE_FOLDER

j2sqb = DefaultSparqlBuilder(DEFAULT_TEMPLATE_FOLDER)


broker = MarineBroker({"https://www.ifremer.fr/erddap": ["ArgoFloats-synthetic-BGC"]})


eovs_request = ["EV_SALIN", "EV_OXY", "EV_SEATEMP"]
assert_vars = ["psal", "doxy", "temp"]
start_date = "2022-01-16"
end_date = "2022-01-17"
# North-east Atlantic Ocean
min_lon = -40
min_lat = 35
max_lon = 2
max_lat = 62

query_filename = "platform_finder.sparql"
variables = {
    "start_date": start_date,
    "end_date": end_date,
    "min_lon": min_lon,
    "min_lat": min_lat,
    "max_lon": max_lon,
    "max_lat": max_lat,
}

qry = j2sqb.build_syntax(query_filename, **variables)


@pytest.mark.parametrize("eov", eovs_request)
def test_query_vocabularies(eov):
    result = broker.query_vocabularies(eov)
    assert isinstance(result, List)
    assert len(result) > 0
    for res in result:
        assert isinstance(res, Dict)


dataset_tests = [(e, a) for (e, a) in zip(eovs_request, assert_vars)]


@pytest.mark.parametrize("eov, assert_var", dataset_tests)
def test_find_eov_in_dataset(eov, assert_var):
    found_vars = broker.find_eov_in_dataset(
        broker.datasets[0], eov, broker.vocabularies[eov]
    )
    assert found_vars[0] == assert_var


def test_submit_sparql_query():
    response = broker.submit_sparql_query(
        qry,
        broker.DEFAULT_SPARQL_ENDPOINTS["Argo"],
        eovs_request,
        start_date,
        end_date,
        min_lon,
        min_lat,
        max_lon,
        max_lat,
        "nc",
        "platform_number",
        "ArgoFloats-synthetic-BGC",
    )
    df = response.compile_results()

    assert len(df) == 5852


def test_submit_sparql_named_query():
    response = broker.submit_sparql_named_query(
        query_filename,
        broker.DEFAULT_SPARQL_ENDPOINTS["Argo"],
        eovs_request,
        "nc",
        "platform_number",
        "ArgoFloats-synthetic-BGC",
        **variables
    )
    df = response.compile_results()

    assert len(df) == 5852
