from pykg2tbl.j2.jinja_sparql_builder import J2SparqlBuilder
from pysubyt.j2.functions import Filters, Functions

DEFAULT_TEMPLATE_FOLDER = "src/marine_eov_broker/j2_templates"

j2sqb = J2SparqlBuilder(
    DEFAULT_TEMPLATE_FOLDER, j2_filters=Filters, j2_functions=Functions
)


EOV_LIST = [
    "EV_SEATEMP",
    "EV_SALIN",
    "EV_OXY",
    "EV_CURR",
    "EV_CHLA",
    "EV_CO2",
    "EV_NUTS",
]


DEFAULT_QUERY_STRINGS = {
    key: j2sqb.build_sparql_query("eov_query.sparql", **{"eov": key})
    for key in EOV_LIST
}
