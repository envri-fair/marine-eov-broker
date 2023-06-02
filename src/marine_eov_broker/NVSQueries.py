from pykg2tbl.j2.jinja_sparql_builder import J2SparqlBuilder

DEFAULT_TEMPLATE_FOLDER = "src/marine_eov_broker/j2_templates"

j2sqb = J2SparqlBuilder(DEFAULT_TEMPLATE_FOLDER)


list_of_EVs = [
    "EV_SEATEMP",
    "EV_SALIN",
    "EV_OXY",
    "EV_CURR",
    "EV_CHLA",
    "EV_CO2",
    "EV_NUTS",
]


query_strings = {
    key: j2sqb.build_sparql_query("EV.sparql", **{"EV_var": key})
    for key in list_of_EVs
}
