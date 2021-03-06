query_strings = {
    "EV_SEATEMP": """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX pav: <http://purl.org/pav/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

select distinct  ?dt ?P01notation ?prefLabel (?R03notation as ?R03) (?P09notation as ?P09) (?P02notation as ?P02)where {<http://vocab.nerc.ac.uk/collection/A05/current/EV_SEATEMP/> 
#<https://w3id.org/env/puv#statistic> ?v ; 
    
<https://w3id.org/env/puv#matrix> ?v1  ;
  <https://w3id.org/env/puv#property> ?v2  .
    

<http://vocab.nerc.ac.uk/collection/P01/current/>  skos:member ?dt  .
  ?dt owl:deprecated ?depr . FILTER((str(?depr)="false"))
 
  #?dt ?rel ?v .  filter(!regex(str(?v),'S07') && regex(str(?rel),'related') ).
  NOT EXISTS {
  ?dt ?rel  ?v .  filter(regex(str(?v),'S07/current/'))
   }
  
     ?dt ?rel1 ?v1 .
       ?dt ?rel2 ?v2 .
  optional {    ?dt ?rel3 ?v3 . filter(regex(str(?v3),'R03/current/')) .  ?v3 skos:notation ?R03notation .}
   optional {    ?dt ?rel4 ?v4 . filter(regex(str(?v4),'P09/current/')) .  ?v4 skos:notation ?P09notation .}
     optional {    ?dt ?rel5 ?v5 . filter(regex(str(?v5),'P02/current/')) .  ?v5 skos:notation ?P02notation .}

?dt skos:prefLabel ?prefLabel .FILTER(langMatches(lang(?prefLabel), "en"))
    ?dt skos:notation ?P01notation .
  
  
}
    """,
    "EV_SALIN": """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX pav: <http://purl.org/pav/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

select distinct  ?dt ?P01notation ?prefLabel (?R03notation as ?R03) (?P09notation as ?P09)(?P02notation as ?P02) where {<http://vocab.nerc.ac.uk/collection/A05/current/EV_SALIN/> 
#<https://w3id.org/env/puv#statistic> ?v ; 
    
<https://w3id.org/env/puv#matrix> ?v1  ;
  <https://w3id.org/env/puv#property> ?v2  .
    

<http://vocab.nerc.ac.uk/collection/P01/current/>  skos:member ?dt  .
  ?dt owl:deprecated ?depr . FILTER((str(?depr)="false"))

  #?dt ?rel ?v .  filter(!regex(str(?v),'S07') && regex(str(?rel),'related') ).
  NOT EXISTS {
  ?dt ?rel  ?v .  filter(regex(str(?v),'S07/current/'))
   }
  
     ?dt ?rel1 ?v1 .
       ?dt ?rel2 ?v2 .
     optional {    ?dt ?rel33 ?v33 . filter(regex(str(?v33),'R03/current/')) . ?v33 skos:notation ?R03notation .}
     optional {    ?dt ?rel4 ?v4 . filter(regex(str(?v4),'P09/current/')) . ?v4 skos:notation ?P09notation .}
     optional {    ?dt ?rel5 ?v5 . filter(regex(str(?v5),'P02/current/')) . ?v5 skos:notation ?P02notation .}

?dt skos:prefLabel ?prefLabel .FILTER(langMatches(lang(?prefLabel), "en"))
?dt skos:notation ?P01notation .  
  
}
    """,
    "EV_OXY": """
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX pav: <http://purl.org/pav/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

select distinct  ?dt ?P01notation ?prefLabel (?R03notation as ?R03) (?P09notation as ?P09) (?P02notation as ?P02)where {<http://vocab.nerc.ac.uk/collection/A05/current/EV_OXY/> 
#<https://w3id.org/env/puv#statistic> ?v ; 
    
<https://w3id.org/env/puv#matrix> ?v1  ;
  <https://w3id.org/env/puv#property> ?v2  ;
 <https://w3id.org/env/puv#chemicalObject> ?v3 .
    

<http://vocab.nerc.ac.uk/collection/P01/current/>  skos:member ?dt  .
  ?dt owl:deprecated ?depr . FILTER((str(?depr)="false"))

  #?dt ?rel ?v .  filter(!regex(str(?v),'S07') && regex(str(?rel),'related') ).
  NOT EXISTS {
  ?dt ?rel  ?v .  filter(regex(str(?v),'S07/current/'))
   }
  
     ?dt ?rel1 ?v1 .
       ?dt ?rel2 ?v2 .
        ?dt ?rel3 ?v3 .
   optional {    ?dt ?rel33 ?v33 . filter(regex(str(?v33),'R03/current/')) . ?v33 skos:notation ?R03notation .}
   optional {    ?dt ?rel4 ?v4 . filter(regex(str(?v4),'P09/current/')) . ?v4 skos:notation ?P09notation .}
     optional {    ?dt ?rel5 ?v5 . filter(regex(str(?v5),'P02/current/')) . ?v5 skos:notation ?P02notation .}

?dt skos:prefLabel ?prefLabel .FILTER(langMatches(lang(?prefLabel), "en")) .
  ?dt skos:notation ?P01notation .
  

}
    """,
    "EV_CURR": """
    """,
    "EV_CHLA": """
    """,
    "EV_CO2": """
    """,
    "EV_NUTS": """
    """
}