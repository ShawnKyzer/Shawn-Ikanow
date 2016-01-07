df <- jsonFile(sqlContext, "/Users/shawnkyzer/Documents/aleph2_analytic_services_R/src/main/resources/people.JSON")
localDf <- collect(df)
text <- capture.output(print(localDf))
print(text)