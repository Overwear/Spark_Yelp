
hadoop fs -rm -r sparkout
spark-submit \
      --class SparkYelp \
      --master yarn-cluster \
      --executor-memory 4G \
      --num-executors 4 \
      --executor-cores 4 \
      /home/lee48493/Yelp.jar \
      /user/lee48493/project/yelp_academic_dataset_review.json \
      sparkout \
      1
