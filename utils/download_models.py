from transformers import TFBertForTokenClassification, BertTokenizer 
import tensorflow as tf
import os

MODEL_NAME = 'dslim/bert-base-NER'

tokenizer = BertTokenizer.from_pretrained(MODEL_NAME)
tokenizer.save_pretrained('./{}_tokenizer/'.format(MODEL_NAME))

# just in case if there is no TF/Keras file provided in the model
# we can just use `from_pt` and convert PyTorch to TensorFlow
try:
  print('try downloading TF weights')
  model = TFBertForTokenClassification.from_pretrained(MODEL_NAME)
except:
  print('try downloading PyTorch weights')
  model = TFBertForTokenClassification.from_pretrained(MODEL_NAME, from_pt=True)

# Define TF Signature
@tf.function(
  input_signature=[
      {
          "input_ids": tf.TensorSpec((None, None), tf.int32, name="input_ids"),
          "attention_mask": tf.TensorSpec((None, None), tf.int32, name="attention_mask"),
          "token_type_ids": tf.TensorSpec((None, None), tf.int32, name="token_type_ids"),
      }
  ]
)
def serving_fn(input):
    return model(input)

model.save_pretrained("./{}".format(MODEL_NAME), saved_model=True, signatures={"serving_default": serving_fn})

asset_path = '{}/saved_model/1/assets'.format(MODEL_NAME)
os.system(f"cp {MODEL_NAME}_tokenizer/vocab.txt {asset_path}")


labels = model.config.label2id
# sort the dictionary based on the id
labels = sorted(labels, key=labels.get)

with open(asset_path+'/labels.txt', 'w') as f:
    f.write('\n'.join(labels))