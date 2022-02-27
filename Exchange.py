import numpy  as np
import pandas as pd
from wallet import wallet

class Exchanges(wallet):
    def __init__(self,init_balance,train,window_size):
      self.Init_balance = init_balance
      self.Train       = train
      self.WindowSize = window_size
      self.codeAsset   = []
      self.dataset     = self.Load_data(self.Train,self.WindowSize)
      super().__init__(self.Init_balance,self.getCodeAsset(),self.getPortionAsset())

     
      
      

    def Load_data(self,train,window_size):
      # ex: import dataset
      self.codeAsset    = ['Asset01','Asset02','Asset03','Asset04','Asset05']
      #update : 25/02/2022 nopanon 
      self.portionAsset = {'Asset01':0.35,'Asset02':0.35,'Asset03':0.35,'Asset04':0.35,'Asset05':0.35}

      #import train
      Asset01_train_df  = pd.read_csv('Asset01_train.csv',index_col='timestamp')
      Asset02_train_df  = pd.read_csv('Asset02_train.csv',index_col='timestamp')
      Asset03_train_df  = pd.read_csv('Asset03_train.csv',index_col='timestamp')
      Asset04_train_df  = pd.read_csv('Asset04_train.csv',index_col='timestamp')
      Asset05_train_df  = pd.read_csv('Asset05_train.csv',index_col='timestamp')
      
      #import test
      Asset01_test_df  = pd.read_csv('Asset01_test.csv',index_col='timestamp')
      Asset02_test_df  = pd.read_csv('Asset02_test.csv',index_col='timestamp')
      Asset03_test_df  = pd.read_csv('Asset03_test.csv',index_col='timestamp')
      Asset04_test_df  = pd.read_csv('Asset04_test.csv',index_col='timestamp')
      Asset05_test_df  = pd.read_csv('Asset05_test.csv',index_col='timestamp')

      if train : return {self.codeAsset[0] : Asset01_train_df,self.codeAsset[1] : Asset02_train_df,self.codeAsset[2] : Asset03_train_df,self.codeAsset[3] : Asset04_train_df,self.codeAsset[4] : Asset05_train_df}
      else:      return {self.codeAsset[0] : pd.concat([Asset01_train_df[-self.WindowSize:],Asset01_test_df]),self.codeAsset[1] : pd.concat([Asset02_train_df[-self.WindowSize:],Asset02_test_df]),self.codeAsset[2] : pd.concat([Asset03_train_df[-self.WindowSize:],Asset03_test_df]),self.codeAsset[3] : pd.concat([Asset04_train_df[-self.WindowSize:],Asset04_test_df]),self.codeAsset[4] : pd.concat([Asset05_train_df[-self.WindowSize:],Asset05_test_df])}
      

    def getDataset(self):
      return self.dataset

    def getCodeAsset(self):
      return self.codeAsset
    #update : 25/02/2022 nopanon : getProtionAsset to getPortionAsset
    def getPortionAsset(self):
      return self.portionAsset
    def getWindowPrice(self,timestep,window):
      return {self.codeAsset[0] : self.getDataset()[self.codeAsset[0]][timestep - window : timestep],
              self.codeAsset[1] : self.getDataset()[self.codeAsset[1]][timestep - window : timestep],
              self.codeAsset[2] : self.getDataset()[self.codeAsset[2]][timestep - window : timestep],
              self.codeAsset[3] : self.getDataset()[self.codeAsset[3]][timestep - window : timestep],
              self.codeAsset[4] : self.getDataset()[self.codeAsset[4]][timestep - window : timestep]}


    def reset(self):
      self.codeAsset = []
      self.dataset   = self.Load_data(self.Train,self.WindowSize)
      super().reset(self.Init_balance,self.getCodeAsset(),self.getPortionAsset())