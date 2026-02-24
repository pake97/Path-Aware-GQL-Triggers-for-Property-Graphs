MATCH (person:Person {id: $personId})
CREATE (person)-[:own]->(account:Account {id: $accountId, createTime: $currentTime, isBlocked: $accountBlocked, type: $accountType})
