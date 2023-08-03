# New fetures

## Aplication of [pykg2tbl](https://github.com/vliz-be-opsci/pykg2tbl)

Using pykg2tbl we outsource two points:
    1. Queries
    2. Endpoint

### Queries

Instead of hardcoding queries, we now build them from a template using [DefaultSparqlBuilder](src/marine_eov_broker/NVSQueries.py#5).
This eliminates the need to have every query hardcoded and copied, allowing for easier modification of specific variables. The templating also enables variable input and facilitates the implementation of new query formats.

### Endpoint

By applying the [KGSource](src/marine_eov_broker/MarineRiBroker.py#14) we have introduced a dynamic endpoint source for querying. The KGSource constructs an object that accepts various sources such as graphs, TTL files, and endpoints. It simplifies the querying process with a consistent and straightforward syntax. For more details, refer to the [implementation](src/marine_eov_broker/MarineRiBroker.py#457).

The KGSource ABS also allows for the creation of additional implementations if the need for more sources arises. 
This implementation has removed all the code connecting to the endpoints and making the queries, outsourcing it all to the object.

#### Eurobis ERDDAP

Added the [eurobis ERDDAP](https://erddap.eurobis.org/erddap), maintained by [VLIZ](https://www.vliz.be/en), as a default endpoint for the broker. 

## I-Adopt

Added a new query [template](src/marine_eov_broker/j2_templates/nsv-eov-to-usage_via-iop.sparql) that follows the I-Adopt concept.


