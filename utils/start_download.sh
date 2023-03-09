MODEL_NAME=dslim/bert-base-NER
python download_models.py $MODEL_NAME
rm -rf models/${MODEL_NAME}_tokenizer
