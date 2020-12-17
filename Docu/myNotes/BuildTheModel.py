import tensorflow as tf
#Define input feature columns
featcols = [
 tf.feature_column.numeric_column("sq_footage") ]
#Instantiate Linear Regression Model
model = tf.estimator.LinearRegressor(featcols, './model_trained')
#Train
def train_input_fn():
 ...
 return features, labels
model.train(train_input_fn, steps=100)
#Predict
def pred_input_fn():
 ...
 return features
out = model.predict(pred_input_fn)

#Encoding categorical data to supply to a Deep Neural Network (DNN)
#If you know the complete vocabulary beforehand:
tf.feature_column.categorical_column_with_vocabulary_list('zipcode',
 vocabulary_list = ['83452', '72345', '87654', '98723', '23451']),
#If your data if already indexed, i.e. has integers in [0-N)
tf.feature_column.categorical_column_with_identity('stateId',
 num_buckets = 50)
#To pass in a categorical column into a DNN, one option is to one-hot encode it:
tf.feature_column.indicator_column( my_categorical_column )

#To read CSV files. create a TextLineDataset giving it a function to decode the CSV into features, labels
CSV_COLUMNS = ['sqfootage','city','amount']
LABEL_COLUMN = 'amount'
DEFAULTS = [[0.0], ['na'], [0.0]]
def read_dataset(filename, mode, batch_size=512):
 def decode_csv(value_column):
 columns = tf.decode_csv(value_column, record_defaults=DEFAULTS)
 features = dict(zip(CSV_COLUMNS, columns))
 label = features.pop(LABEL_COLUMN)
 return features, label

 dataset = tf.data.TextLineDataset(filename).map(decode_csv)
 ...
 return ...

#Shuffling is important for distributed training
def read_dataset(filename, mode, batch_size=512):
 ...
 dataset = tf.data.TextLineDataset(filename).map(decode_csv)
 if mode == tf.estimator.ModeKeys.TRAIN:
 num_epochs = None # indefinitely
 dataset = dataset.shuffle(buffer_size=10*batch_size)
 else:
 num_epochs = 1 # end-of-input after this
 dataset = dataset.repeat(num_epochs).batch(batch_size)

 return dataset.make_one_shot_iterator().get_next()

 #TrainSpec consists of the things that used to be passed into train() method
 
 train_spec = tf.estimator.TrainSpec(
 input_fn=read_dataset('gs://.../train*', mode=tf.contrib.learn.ModeKeys.TRAIN),
 max_steps=num_train_steps)
...
tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)

#EvalSpec controls the evaluation and the checkpointing of the model because they happen at the same time

exporter = ...
eval_spec=tf.estimator.EvalSpec(
 input_fn=read_dataset('gs://.../valid*', mode=tf.contrib.learn.ModeKeys.EVAL),
 steps=None,
 start_delay_secs=60, # start evaluating after N seconds
 throttle_secs=600, # evaluate every N seconds
 exporters=exporter)
tf.estimator.train_and_evaluate(estimator, train_spec, eval_spec)
