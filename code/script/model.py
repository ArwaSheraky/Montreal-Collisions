if __name__ == "__main__":

	try:
		from pyspark import SparkContext, SparkConf
		from pyspark.sql import SparkSession
	#    from pyspark import SparkFiles
		from operator import add
	except Exception as e:
		print(e)

	import pandas as pd
	import numpy as np
	from sklearn.linear_model import LinearRegression
	from sklearn.ensemble import RandomForestRegressor
	from sklearn.model_selection import train_test_split

	from sklearn.model_selection import KFold

	from sklearn.preprocessing import normalize
	from sklearn.preprocessing import StandardScaler

	from sklearn.model_selection import cross_val_score

	# sklearn :: evaluation metrics
	from sklearn.metrics import mean_absolute_error
	from sklearn.metrics import mean_squared_error


	spark = SparkSession.builder \
				.master('local[1]') \
				.appName("buildingPredictionModel") \
				.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('WARN')

	X = pd.read_csv('//app/final_part_1.csv')
	# X = spark.read.csv('file:///app/final_part_1.csv', header='true', inferSchema = True)
	# print(X.show(5))
	del X['date']
	# X.drop('date')

	y = X['count']
	del X['count']
	# X.drop('count')

	# print(X.show(5))

	list_columns=[]
	for i in X.columns:
		list_columns.append(i)

	# scores = cross_val_score(RandomForestRegressor(500), X, y, cv=10)
	# print('cross_val_score', np.mean(scores))

	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
	print('X_train: ', X_train.shape, 'y_train:', y_train.shape, '\nX_test: ', X_test.shape, 'y_test: ', y_test.shape)

	model = RandomForestRegressor(100)
	model.fit(X_train, y_train)
	y_pred = model.predict(X_test)

	fi = []
	for i, col in enumerate(X_test.columns):
		fi.append([col, model.feature_importances_[i]])
	features = pd.DataFrame(fi).sort_values(1, ascending=False)
	df_features = spark.createDataFrame(features)
	print(df_features.show(5))

	# from sklearn.externals import joblib
	# joblib.dump(model, 'model.joblib')

	df_features.write.save("hdfs://hadoop:8020/user/me/feature_importances", format='csv', mode='append')
	# features.to_csv('//app/features.csv', index=False) 
	# feature_lines = sc.textFile("//app/features.csv")
	# feature_lines.saveAsTextFile("hdfs://hadoop:8020/user/me/features")

	sc.stop()



