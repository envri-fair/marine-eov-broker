from pyrdfj2 import J2RDFSyntaxBuilder

DEFAULT_TEMPLATE_FOLDER = "src/marine_eov_broker/j2_templates"

j2sqb = J2RDFSyntaxBuilder(DEFAULT_TEMPLATE_FOLDER)


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
    key: j2sqb.build_syntax("eov_query.sparql", **{"eov": key}) for key in EOV_LIST
}
