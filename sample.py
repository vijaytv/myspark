from pyspark.sql.functions import array_contains

#Initialize SQLContext.  Needs this if file not stoed in Hive
sqlContext = SQLContext(sc)

#Read in the Businesses from JSOn file
bizfile = "yelp_academic_dataset_business.json"
reviewsfile = "yelp_academic_dataset_review.json"

reviews = sqlContext.read.json(reviewsfile)


biz = sqlContext.read.json(bizfile)


#Verify Schema
biz.printSchema()


#Get All Categories
categories = biz.flatMap(lambda x: x.categories).distinct().collect()

#This does not work in Spark if Hive is not present.
#contains = udf(lambda xs, val: val in xs, BooleanType())

#Filter all business with category of filter
#pizzerias = biz.filter(array_contains(biz.categories,"Pizza"))

biz.registerTempTable("businesses")

pizzerias  = sqlContext.sql("""select * from businesses where lower(name) like '%pizza%' or array_contains(categories,'Pizza') """)

pizzerias.cache()

allAvgrating = reviews.groupBy('business_id').agg({'stars': 'mean'}).withColumnRenamed("avg(stars)", "avgrating")

pizzaReviews = pizzerias.join(allAvgrating, 'business_id').select(pizzerias.name, allAvgrating.avgrating)
