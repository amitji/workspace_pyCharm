
# coding: utf-8

# # Creating word vector for word2 vec demo

# In[8]:


import nltk 
from nltk import word_tokenize, sent_tokenize
import gensim
from gensim.models.word2vec import Word2Vec
from sklearn.manifold import TSNE
import pandas as pd
from bokeh.io import output_notebook
from bokeh.plotting import show, figure
get_ipython().magic('matplotlib inline')



# In[9]:


nltk.download('punkt')


# #### Load data

# In[12]:


nltk.download('gutenberg')


# In[14]:


from nltk.corpus import gutenberg


# In[16]:


gutenberg.fileids()


# #### Tokenize  

# In[17]:


gberg_sent_tokens = sent_tokenize(gutenberg.raw())


# In[18]:


gberg_sent_tokens[0:5]


# In[20]:


word_tokenize(gberg_sent_tokens[1])


# In[21]:


word_tokenize(gberg_sent_tokens[1])[14]


# In[22]:


gberg_sents = gutenberg.sents()


# In[23]:


gberg_sents[0:5]


# In[24]:


gberg_sents[4]


# In[25]:


gberg_sents[4][14]


# In[26]:


gutenberg.words()


# In[27]:


#model = Word2Vec(sentences=gberg_sents, size=64,sg=1,window=10,min_count=5,seed=42,workers=8)
#model.save('raw_gutenberg_model.w2v')


# In[33]:


model= gensim.models.Word2Vec.load('raw_gutenberg_model.w2v')


# In[34]:


model['dog']


# In[35]:


len(model['dog'])


# In[38]:


model.most_similar('dog')


# In[39]:


model.most_similar('day')


# In[40]:



model.most_similar('father')


# In[43]:


model.doesnt_match('mother father son daughter dog'.split())


# In[48]:


model.most_similar(positive=['husband', 'woman'], negative=['man'])


# #### Reduce the vector dimentionality with t-SNE

# In[49]:


len(model.wv.vocab)


# In[53]:


X = model[model.wv.vocab]


# In[54]:


tsne = TSNE(n_components=2,n_iter=1000)


# In[55]:


X_2d = tsne.fit_transform(X)


# In[59]:


coords_df = pd.DataFrame(X_2d,columns=['x','y'])
coords_df['tokens'] = model.wv.vocab.keys()


# In[60]:


coords_df.head()


# In[61]:


coords_df.to_csv('raw_gutenberg_tsne.csv', index=False)


# In[62]:


coords_df = pd.read_csv('raw_gutenberg_tsne.csv')


# In[64]:


_ = coords_df.plot.scatter('x', 'y', figsize=(8,8), marker='.', s=10, alpha=0.2)


# In[65]:


output_notebook()


# In[68]:


subset_df = coords_df.sample(n=5000)


# In[69]:


p = figure(plot_width=800, plot_height=800)
_ = p.text(x=subset_df.x, y=subset_df.y, text=subset_df.tokens)


# In[71]:


show(p)

