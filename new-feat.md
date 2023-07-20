# New fetures

## Aplication of [pykg2tbl](https://github.com/vliz-be-opsci/pykg2tbl)

Using pykg2tbl we outsource two points:
    1. Queries
    2. Endpoint

### Queries

The queries instead of being hardcoded are now built from a template using [DefaultSparqlBuilder](src/marine_eov_broker/NVSQueries.py#5).
This will remove the need to have every query hardcoded and copied, changing only the specific variables, since the templating also allows for variable input.
Also this method allow for easier implementation of new query formats.

### Endpoint

Aplying the [KGSource](src/marine_eov_broker/MarineRiBroker.py#14) allows for a dynamic endpoint source to be queried.
The KGSource will construct an object that will accept several sources (Graphs, ttl files, endpoints) and manage to do queries with the same simple sintax. See [implementation](src/marine_eov_broker/MarineRiBroker.py#457).

The KGSource ABS also allows for more impletations to be created if the need for more sources surfaces.

This implementation removed all the code connecting to the endpoints and making the queries, outsourcing it all to the object.

## I-Adopt

Added a new query [template](src/marine_eov_broker/j2_templates/nsv-eov-to-usage_via-iop.sparql) that follows the I-Adopt concept.


