#### NOTES ####
{
  # Relationship type as a property
  
}


# Graph the relationships between concepts in the Snomed dataset from National Library of Medicine



#--- Setup ----
{
  # Only run one time
  install.packages("devtools")
  library(devtools)
  install_github("davidlrosenblum/neo4r")
  
  list.packages <- c("plyr","dplyr","data.table","sqldf","caret", "readxl","neo4r")
  new.packages <- list.packages[!(list.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) install.packages(new.packages)
  lapply(list.packages, require, character.only = T)
  
  
  options(scipen = 999)
  
  
}

#--- Read Data ----
{
  setwd("C:\\Users\\TimEa\\OneDrive\\Data\\Snomed\\Snapshot\\Terminology")
  # concepts = read.delim("sct2_Concept_Snapshot_US1000124_20150301.txt")
  relationships = read.delim("sct2_Relationship_Snapshot_US1000124_20150301.txt")
  description = read.delim("sct2_Description_Snapshot-en_US1000124_20150301.txt")
  # text_definition = read.delim("sct2_TextDefinition_Snapshot-en_US1000124_20150301.txt")
  # stated_relationship = read.delim("sct2_StatedRelationship_Snapshot_US1000124_20150301.txt")
  
  
}

#--- Test Example ----
{
  
  head(relationships)
  
  description %>%
    filter(conceptId == 255116009)
  
  description %>%
    filter(conceptId == 367639000)
  
  description %>%
    filter(conceptId == 308489006)
  
}


#--- Join Data ----
{
  n = description %>%
    select(conceptId, term) %>%
    filter(is.na(term)==F)
    # select(conceptId, typeId) %>%
    # filter(is.na(typeId) == F)
    # select(conceptId, term)
    # select(conceptId)
  
  n$term = substr(n$term, 1, 100)
  head(n)
  n$term = gsub(',',"",n$term)
  n$term = gsub("'","",n$term)
  n$term = gsub('"',"",n$term)
  
  r = relationships %>%
    select(SRC_ID = sourceId, TGT_ID = destinationId, typeId)
  
  # this adds records.  We should read documentation and join differently
  r = inner_join(r,n,by=c("typeId"="conceptId")) 
  r = r %>% rename(rel_term = term, rel_typeId = typeId)
  
  # Add names for SRC and TGT
  r = inner_join(r,n,by=c("SRC_ID"="conceptId"))
  r = r %>% rename(SRC_TERM = term)
  
  r = inner_join(r,n,by=c("TGT_ID"="conceptId"))
  r = r %>% rename(TGT_TERM = term)
  
}

#--- Small Graph ----
{
  n2 = head(n,n=10000)
  
  r2 = r %>%
    filter(SRC_ID %in% n2$conceptId | TGT_ID %in% n2$conceptId)
}

#--- Write Data ----
{
  wd = paste0("C:\\Users\\TimEa\\AppData\\Local\\Neo4j\\Relate\\Data\\dbmss\\dbms-12f9c10e-aaff-4611-9a63-4886920e7aef\\import")
  setwd(wd)
  
  # fwrite(n2, "nodes.csv", row.names=FALSE)
  fwrite(r, "edges.csv", row.names=FALSE)
}

#--- Write to Neo4j ----
{
  # install.packages("neo4r")
  library(neo4r)
  
  packageVersion('neo4r')
  
  con <- neo4j_api$new(
    url = "http://localhost:7474", 
    user = "neo4j", 
    password = "1Futbol1"
  )
  con$ping()
  con$get_version()
  
  
  # delete the database 
  call_neo4j(con
             , include_stats = T, query = paste0("
             Match (a) detach delete a
  "))

  

  
  start <- proc.time()
  
  i = 1
  batch_size = 50000
  j = batch_size
  
  while (j < nrow(r)+batch_size) {
    r3 = r[i:j,]
    fwrite(r3, "edges.csv", row.names=FALSE)
    
    # Write edges
    call_neo4j(con
               , include_stats = T, query = paste0("
             
      WITH 'file:///edges.csv' AS uri
      LOAD CSV WITH HEADERS FROM uri AS row FIELDTERMINATOR ',' 
      MERGE (source:Concept {conceptId: row.SRC_ID, term: row.SRC_TERM})
      MERGE (target:Concept {conceptId: row.TGT_ID, term: row.TGT_TERM})
      WITH source, target, row
      CALL apoc.create.relationship(source, row.rel_term, {}, target) YIELD rel
      RETURN *
      ;
    "))
    print(paste0("Finished loading ",i," through ", j))
    i = i + batch_size
    j = j + batch_size
  }
 
  
  end <- proc.time()
  print(end - start)
  
  # Set Index
  call_neo4j(con
             , include_stats = T, query = paste0("
  CREATE INDEX ON :Concept(conceptId)
  //CREATE INDEX conceptId IF NOT EXISTS FOR (n:Concept) ON (n.conceptId)
  ;
  "))
  
  # Set Index
  call_neo4j(con
             , include_stats = T, query = paste0("
  CREATE INDEX ON :Concept(term)
  "))
  
  
}

# call_neo4j(con
#            , include_stats = T, query = "
# LOAD CSV WITH HEADERS FROM 'https://github.com/timothyEastridge/US_Patents/blob/main/nodes.csv' AS row
# RETURN row
# LIMIT 5;
# ")

# call_neo4j(con
#            , include_stats = T, query = "
# CREATE CONSTRAINT unique_record_id ON (p:id) ASSERT p.id IS UNIQUE;
# ")



'file:///artists.csv'

# Write Record ID numbers
call_neo4j(con
           , include_stats = T, query = "
LOAD CSV WITH HEADERS FROM '",wd,"' AS row
MERGE (n:Phrase {id: (row.id)})
  ON CREATE SET n.Phrase = (row.Phrase)
  , n.anchor_or_target = (row.Anchor_or_Target)
  //, n.verse = toInteger(row.verse)
  //, n.text = row.text
  //, n.Scripture_Passage = row.Scripture_Passage
RETURN count(n);

")


# # add Label
# call_neo4j(con
#            , include_stats = T, query = '
# CALL apoc.periodic.iterate(
#   "MATCH (n:Verse) WHERE EXISTS (n.verse_id) return n",
#   "SET n:Verse",
#   {batchsize:10000, parallel:true, processors:4}
# )
# ')


# Index the nodes
call_neo4j(con
           , include_stats = T, query = "
           
CREATE INDEX ON :Phrase(Phrase)

")

# Index the nodes
call_neo4j(con
           , include_stats = T, query = "
           
CREATE INDEX ON :Phrase(id)

")

# # add Label
call_neo4j(con
           , include_stats = T, query = '
CALL apoc.periodic.iterate(
  "MATCH (n:Concept) return n",
  "SET n:Term",
  {batchsize:10000, parallel:true, processors:4}
)
')


# :schema




# Insert relationships


call_neo4j(con
           , include_stats = T, query = "
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/timothyEastridge/US_Patents/main/edges.csv?token=GHSAT0AAAAAABUZDSU5YDXKKWKC56ZHMWUGYUSMUFA' AS row
WITH row 
MATCH (a:Phrase {id: (row.SRC_ID)})
MATCH (b:Phrase {id: (row.TGT_ID)})
MERGE (a)-[rel:CONTEXT {context: row.context, score: row.score, dataset: row.dataset}]->(b)
RETURN count(rel);
")













# Visualize
library(ggraph)
library(magrittr)
library(visNetwork)
library(purrr)
# library(scran)
library(tidyr)
G <-paste0("
MATCH (a:Concept)-[r]-(c:Concept)
where a.term = 'Entire bone organ of thorax (body structure)'
return a,r,c") %>% 
  call_neo4j(con, type = "graph") 

G$nodes <- G$nodes %>%
  unnest_nodes(what = "properties") %>% 
  # We're extracting the first label of each node, but 
  # this column can also be removed if not needed
  mutate(label = map_chr(label, 1))
head(G$nodes)

G$relationships = G$relationships %>%
  unnest_relationships() %>%
  select(startNode, endNode, type, everything()) 

nodes = G$nodes
edges = G$relationships

n = nodes %>% select(id, label = term, value = pagerank)
e = edges %>% select(from = startNode, to = endNode, label = type)

head(n)
head(e)

visNetwork(
  n, 
  e
) %>%
  visPhysics(stabilization = TRUE, enabled = TRUE) %>%
  visOptions(highlightNearest = list(enabled = T, degree = 1, hover = F), autoResize = TRUE, collapse = FALSE) %>%
  visEdges(color = list(highlight = "red")) %>% # The colour of the edge linking nodes
  visLayout(improvedLayout = TRUE) %>%
  visEdges(arrows = edges$arrows) %>%
  visInteraction(multiselect = F)

# G$relationships <- G$relationships %>%
#   unnest_relationships() %>%
#   select(startNode, endNode, type, everything()) %>%
#   mutate(roles = unlist(roles))
head(G$relationships)


graph_object <- igraph::graph_from_data_frame(
  d = G$relationships, 
  directed = TRUE, 
  vertices = G$nodes
)
plot(graph_object)


# We'll just unnest the properties
G$nodes <- G$nodes %>%
  unnest_nodes(what = "properties")
head(G$nodes)  

# Turn the relationships :
G$relationships <- G$relationships %>%
  unnest_relationships() %>%
  select(from = startNode, to = endNode, label = type)
head(G$relationships)

visNetwork::visNetwork(G$nodes, G$relationships)


network <- paste0("
MATCH (a:Concept)-[r]-(c:Concept)
where a.term = 'Entire bone organ of thorax (body structure)'
return a,r,c") %>%
  call_neo4j(con, type = "graph") %>% 
  convert_to("visNetwork")
visNetwork::visNetwork(G$nodes, G$relationships)
