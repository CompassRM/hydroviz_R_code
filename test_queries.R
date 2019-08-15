
# dbGetQuery(connection, "SELECT * FROM alternatives WHERE alternative IN ('RAS_ALT3A_MECH_20170825','Res_Ftpk2_SpawnCue_20190523')")
# dbGetQuery(connection, "SELECT * FROM alternatives WHERE alternative = 'RAS_ALT3A_MECH_20170825'")

TEST <- dbGetQuery(connection, "SELECT * FROM alternatives")

# CREATE STRING OF ALL VALUES TO SEARCH FOR
test_list <- TEST$alternative
test_list <- paste0('\'', paste(test_list, collapse='\',\''), '\'')

# test_list <- as.list(test_list)

# QUERY USING THE STRING
dbGetQuery(connection, paste0("SELECT * FROM alternatives WHERE alternative IN (", test_list, ")"))


# THIS CRASHES EVERY TIME - see if there is another way to use a wildcard to refer to a list directly
# rather than creating a string every time

# test_list2 <- as.list(TEST$alternative)
# Df <- dbGetQuery(con, sprintf("SELECT * FROM EMP WHERE ename %s", likevars))
# dbGetQuery(connection, "SELECT * FROM alternatives WHERE alternative IN %s", test_list2)


# TESTING WITH WILDCARDS
# # Executing the same statement with different values at once
# iris_result <- DBI::dbSendQuery(connection, "SELECT * FROM alternatives WHERE id = %s")
# # iris_result <- dbSendStatement(con, "DELETE FROM iris WHERE [Species] = $species")
# dbBind(iris_result, list(id = c(2, 3,5)))
# dbGetRowsAffected(iris_result)
# dbClearResult(iris_result)
# 
# nrow(dbReadTable(con, "iris"))
# 
# 
# # Pass multiple sets of values with dbBind():
# rs <- dbSendQuery(connection, "SELECT * FROM alternatives WHERE alternative = ?", param = 'RAS_ALT3A_MECH_20170825')
# dbBind(rs, list(6L))
# dbFetch(rs)
# dbBind(rs, list(8L))
# dbFetch(rs)
# dbClearResult(rs)

