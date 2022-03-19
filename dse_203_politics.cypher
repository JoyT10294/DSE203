CREATE (politician:Politician)
CREATE (state:State)
CREATE (party:Party)
CREATE (chamber:Chamber)
CREATE (bill:Bill)
CREATE (committee:Committee)
CREATE (date:Date);

CREATE CONSTRAINT ON (politician:Politician) ASSERT politician.rep_id IS UNIQUE;
CREATE CONSTRAINT ON (state:State) ASSERT state.state_id IS UNIQUE;
CREATE CONSTRAINT ON (state:State) ASSERT state.state_name IS UNIQUE;
CREATE CONSTRAINT ON (party:Party) ASSERT party.party_name IS UNIQUE;
CREATE CONSTRAINT ON (chamber:Chamber) ASSERT chamber.chamber_name IS UNIQUE;
CREATE CONSTRAINT ON (bill:Bill) ASSERT bill.bill_id IS UNIQUE;
CREATE CONSTRAINT ON (bill:Bill) ASSERT bill.bill_name IS UNIQUE;
CREATE CONSTRAINT ON (committee:Committee) ASSERT committee.committee_id IS UNIQUE;
CREATE CONSTRAINT ON (committee:Committee) ASSERT committee.committee_name IS UNIQUE;
CREATE CONSTRAINT ON (date:Date) ASSERT date.date_id IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///state_rep_to_neo.csv' AS repdata
with repdata where repdata.representativeid is not null
MERGE (p:Politician {rep_id: repdata.representativeid, replastname: repdata.lastname, repfirstname: repdata.firstname});
match (p:Politician) WHERE NOT (EXISTS (p.rep_id)) DETACH DELETE p;

LOAD CSV WITH HEADERS FROM 'file:///states_to_neo.csv' AS repdata
with repdata where repdata.statelabel is not null
MERGE (state:State {state_id: repdata.statecode, state_name: repdata.statelabel});
match (s:State) WHERE NOT (EXISTS (s.state_name)) DETACH DELETE s;

Create (party:Party {party_name: 'Democratic'});
Create (party:Party {party_name: 'Republican'});
Create (party:Party {party_name: 'Unaffiliated'});
Create (party:Party {party_name: 'Independent'});
match (p:Party) WHERE NOT (EXISTS (p.party_name)) DETACH DELETE p;

Create (chamber:Chamber {chamber_name: 'Senate'});
Create (chamber:Chamber {chamber_name: 'House of Representatives'});
match (c:Chamber) WHERE NOT (EXISTS (c.chamber_name)) DETACH DELETE c;

LOAD CSV WITH HEADERS FROM 'file:///committees_to_neo.csv' AS repdata
with repdata where repdata.committeename is not null
MERGE (committee:Committee {committee_id: repdata.committeeid, committee_name: repdata.committeename});
match (c:Committee) WHERE NOT (EXISTS (c.committee_name)) DETACH DELETE c;

LOAD CSV WITH HEADERS FROM 'file:///xml_bill_main.csv' AS repdata
with repdata where repdata.billid is not null
MERGE (bill:Bill {bill_id: repdata.billid, bill_name: repdata.billname, bill_title: repdata.officialtitle, bill_type: repdata.legistype});
match (b:Bill) WHERE NOT (EXISTS (b.bill_name)) DETACH DELETE b;

LOAD CSV WITH HEADERS FROM 'file:///dates_to_neo.csv' AS repdata
with repdata where repdata.date is not null
MERGE (date:Date {date_id: repdata.date});
match (d:Date) WHERE NOT (EXISTS (d.date_id)) DETACH DELETE d;

LOAD CSV WITH HEADERS FROM "file:///comm_rel_relationships_to_neo.csv" as input
MATCH (pol:Politician {rep_id: input.representativeid}), (c:Committee {committee_id: input.committeeid})
CREATE (pol)-[:APPOINTED_TO {chair: input.chair, chamber: input.chamber, affiliation: input.party}]->(c);

LOAD CSV WITH HEADERS FROM "file:///state_rep_to_neo.csv" as input
MATCH (p:Politician {rep_id: input.representativeid}), (s:State {state_name: input.statelabel})
CREATE (p)-[:REPRESENTS {district:input.district, title: input.chamber, affiliation: input.party}]->(s);

LOAD CSV WITH HEADERS FROM "file:///state_rep_to_neo.csv" as input
MATCH (pol:Politician {rep_id: input.representativeid}), (party:Party {party_name: input.party})
CREATE (pol)-[:AFFILIATES_WITH {significance: input.chamber}]->(party);

LOAD CSV WITH HEADERS FROM "file:///state_rep_to_neo.csv" as input
MATCH (pol:Politician {rep_id: input.representativeid}), (c:Chamber {chamber_name: input.chamber})
CREATE (pol)-[:SERVES_IN {state: input.state, affiliation: input.party}]->(c);

LOAD CSV WITH HEADERS FROM "file:///stage_rel_to_neo.csv" AS line
WITH line
MATCH (b:Bill {bill_id: line.billid}), (c:Chamber {chamber_name: line.chambername})
with b, c, line
CALL apoc.create.relationship(b, line.billstage, {}, c) YIELD rel return rel;

LOAD CSV WITH HEADERS FROM "file:///date_rel_to_neo.csv" AS line
WITH line
MATCH (b:Bill {bill_id: line.billid}), (d:Date {date_id: line.date})
with b, d, line
CALL apoc.create.relationship(b, line.billstage, {}, d) YIELD rel return rel;

LOAD CSV WITH HEADERS FROM "file:///spon_rel_data_to_neo_reps.csv" AS line
WITH line
MATCH (b:Bill {bill_id: line.billid}), (p:Politician {rep_id: line.representativeid})
CREATE (c)-[:SPONSORED {sponsor_type: line.primaryorco}]->(p)


LOAD CSV WITH HEADERS FROM 'file:///catpd.csv' AS entity
with entity 
CREATE (new:entity.WordTextLower {entity_name: entity.WordTextLower);
CREATE CONSTRAINT ON (new:entity.WordTextLower) ASSERT entity.entity_name IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///allpd3.csv' AS sov
CREATE (sov.sub)-[:sov.verb]->(sov.pred)
