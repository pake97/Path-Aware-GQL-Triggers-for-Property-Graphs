MATCH (loan:Loan {id: $loanId}), (account:Account {id: $accountId})
CREATE (loan)-[:deposit {time: $currentTime, amount: $amount}]->(account)
