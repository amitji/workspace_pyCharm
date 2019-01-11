
# coding: utf-8

# # Vanilla LSTM in Keras

# In this notebook, we use an LSTM to classify IMDB movie reviews by their sentiment.

# #### Load dependencies

# In[1]:


import keras
from keras.datasets import imdb
from keras.preprocessing.sequence import pad_sequences
from keras.models import Sequential
from keras.layers import Dense, Dropout, Embedding, SpatialDropout1D
from keras.layers import LSTM # new! 
from keras.callbacks import ModelCheckpoint
import os
from sklearn.metrics import roc_auc_score 
import matplotlib.pyplot as plt 
get_ipython().magic('matplotlib inline')


# #### Set hyperparameters

# In[2]:


# output directory name:
output_dir = 'model_output/vanillaLSTM'

# training:
epochs = 1 # 4
batch_size = 128

# vector-space embedding: 
n_dim = 64 
n_unique_words = 10000 
max_review_length = 100 # lowered due to vanishing gradient over time
pad_type = trunc_type = 'pre'
drop_embed = 0.2 

# LSTM layer architecture:
n_lstm = 256 
drop_lstm = 0.2

# dense layer architecture: 
# n_dense = 256
# dropout = 0.2


# #### Load data

# In[3]:


(x_train, y_train), (x_valid, y_valid) = imdb.load_data(num_words=n_unique_words) # removed n_words_to_skip

#Amit - see actual review.. just for debugging
INDEX_FROM=3
word_to_id = keras.datasets.imdb.get_word_index()
word_to_id = {k:(v+INDEX_FROM) for k,v in word_to_id.items()}
word_to_id["<PAD>"] = 0
word_to_id["<START>"] = 1
word_to_id["<UNK>"] = 2

id_to_word = {value:key for key,value in word_to_id.items()}
print(' '.join(id_to_word[id] for id in x_train[0] ))


# #### Preprocess data

# In[4]:


x_train = pad_sequences(x_train, maxlen=max_review_length, padding=pad_type, truncating=trunc_type, value=0)
x_valid = pad_sequences(x_valid, maxlen=max_review_length, padding=pad_type, truncating=trunc_type, value=0)


# #### Design neural network architecture

# In[5]:


model = Sequential()
model.add(Embedding(n_unique_words, n_dim, input_length=max_review_length)) 
model.add(SpatialDropout1D(drop_embed))
model.add(LSTM(n_lstm, dropout=drop_lstm))
# model.add(Dense(n_dense, activation='relu')) # typically don't see top dense layer in NLP like in 
# model.add(Dropout(dropout))
model.add(Dense(1, activation='sigmoid'))


# In[6]:


model.summary() 


# #### Configure model

# In[7]:


model.compile(loss='binary_crossentropy', optimizer='adam', metrics=['accuracy'])


# In[8]:


modelcheckpoint = ModelCheckpoint(filepath=output_dir+"/weights.{epoch:02d}.hdf5")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# #### Train!

# In[ ]:


# go have a gander at nvidia-smi
# 85.2% validation accuracy in epoch 2
model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs, verbose=1, validation_data=(x_valid, y_valid), callbacks=[modelcheckpoint])


# #### Evaluate

# In[10]:


model.load_weights(output_dir+"/weights.01.hdf5") # zero-indexed


# In[11]:


y_hat = model.predict_proba(x_valid)


# In[12]:


plt.hist(y_hat)
_ = plt.axvline(x=0.5, color='orange')


# In[13]:


"{:0.2f}".format(roc_auc_score(y_valid, y_hat)*100.0)

