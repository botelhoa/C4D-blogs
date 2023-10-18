
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from tqdm import tqdm
from sklearn.metrics import mean_absolute_error
from scipy.optimize import curve_fit

class LinearInterpolator:
    
    def __init__(self, engagement_col: str='engagements'):
        self.engagement_col = engagement_col
        self.time_col = "times"
        
    
    def interpolate(self, df: pd.DataFrame):
        
        time_list = []
        engagement_list = []
        
        for i in tqdm(range(len(df))):
            
            time_i = df.loc[i, self.time_col].copy()
            engagement_i = df.loc[i, self.engagement_col].copy()
            
            times, new_engagements = self._interpolate(time_i, engagement_i)
            
            time_list.append(times)
            engagement_list.append(new_engagements)
            
        return time_list, engagement_list
            
                
    def _interpolate(self, times: np.array, engagements: np.array):

        for i in range(int(max(times))):
            
            if i not in times:

                insert_val = 0 if i==0 else None
                engagements = np.insert(engagements, i, insert_val, axis=0)
                times = np.insert(times, i, i, axis=0)


        new_engagements = pd.Series(engagements, index=times) \
                                .interpolate(method='index') \
                                .round() \
                                .astype(int) \
                                .to_numpy()

        return times, new_engagements


    def predict(self, engagements: np.array, indices: np.array) -> np.array:
        return engagements[indices]


    def score(self, y: np.array, preds: np.array) -> float:
        return mean_absolute_error(y, preds) 
    
    def evaluate(self, train: pd.DataFrame, test: pd.DataFrame) -> float:

        prediction_count = 0
        scores = 0

        for i in tqdm(range(len(test))):

            engagements = train.loc[i, "interpolated_engagements"]
            times = test.loc[i, self.time_col].astype(int)
            times = times[np.abs(times) < len(engagements)] # Do this because sometimes randomly split test data is outside the interpolation window
            
            y = test.loc[i, self.engagement_col][:len(times)]

            preds = self.predict(engagements, times)

            if preds.size != 0: # Sometimes there are no predictions made due to filtering
                score = self.score(y, preds)
    
                prediction_count += len(preds)
                scores += score

        return scores / prediction_count


    def plot_predictions(self, train_row, test_row):
    
        x = test_row[self.time_col].astype(int)
        x = x[np.abs(x) < len(train_row["interpolated_engagements"])]
        y = test_row[self.engagement_col][:len(x)]

        ax = plt.axes()
        ax.scatter(x, y, color='red', label='Actual Observations')
        ax.plot(train_row["complete_time"], train_row["interpolated_engagements"], 'k', label='Interpolated Time Series')
        ax.set_ylabel('Engagement')
        ax.set_xlabel(self.time_col)
        ax.set_ylim(0)
        ax.set_xlim(0)
        ax.legend()
        plt.show()



class CurveFitter:
    
    def __init__(self,):
        
        self.fits = {
            "linear": {"function": self.linear, "params": None},
            "sigmoid": {"function": self.sigmoid, "params": None},
            "logarithmic": {"function": self.logarithmic, "params": None},
            "exponential": {"function": self.exponential, "params": None},
        }
        self.time_col = "times"
        

    def fit(self, x, y):

        best_score = None
        self.best_fit = None
        
        for i in self.fits:

            # Reset logarthmic changes
            x = x
            y = y
            
            try:

                if i == "logarithmic":
                    x = np.delete(x, 0)
                    y = np.delete(y, 0)
                
                params, _ = curve_fit(
                    f=self.fits[i]["function"], 
                    xdata=x, 
                    ydata=y, 
                    p0=self._initialize(x, y, i),
                    #bounds = self._bound(y),
                    method='dogbox',
                )
 
                score = self.score(fit=i, params= params, x=x, y=y)

                if best_score == None or score < best_score:
                    best_score = score
                    self.best_fit = i
                    self.fits[i]["params"] = params


            except RuntimeError : #Optimal parameters not found: The maximum number of function evaluations is exceeded.
                pass
            
            except ValueError: # ValueError: array must not contain infs or NaNs - get thrown incorrectly
                pass
            
            except np.linalg.LinAlgError: # SVD did not converge in Linear Least Squares
                pass
                
            

    def _initialize(self, x, y, fit_type: str):
        
        if fit_type == "sigmoid":
            return [max(y), np.median(x), 1, min(y)]
            
        elif fit_type == "logarithmic":
            return [6, 1.5, 0.2]
        
        elif fit_type == "exponential":
            return [5, 0.6, 40]
            
        else:
            return None
        
        
    def _bound(self, y,):
        return (0, max(y))
        
    
    def sigmoid(self, x, L, x0, k, b):
        y = L / (1 + np.exp(-k*(x-x0))) + b
        return (y)

    
    def linear(self, x, a, b):
        return a * x + b

    
    def logarithmic(self, x, a, b, c):
        return a * np.log(x - c) + b

    
    def exponential(self, x, a, b, c):
        return a * np.exp(-b * x) + c
        

    def predict(self, fit: str, params: list, x: list) -> list:
        return np.nan_to_num(self.fits[fit]["function"](*[x]+list(params)))

    def score(self, fit: str, params: list, x: list, y: list) -> float:
        return mean_absolute_error(y, self.predict(fit, params, x)) 


    def plot_predictions(self, train_row, test_row):
    
        x = np.concatenate((train_row["times"], test_row["times"] ), axis=0)
        y = np.concatenate((train_row["engagements"], test_row["engagements"] ), axis=0)
        
        params = train_row["parameters"]
        fit = train_row["fit_type"]
    
        x_fitted = np.linspace(0, np.max(x), 100)
        y_fitted = self.predict(fit=fit, params=params, x=x_fitted)
        
        ax = plt.axes()
        ax.scatter(train_row["times"], train_row["engagements"], color='blue', label='Train Engagement')
        ax.scatter(test_row["times"], test_row["engagements"], color='red', label='Test Engagement')
        ax.plot(x_fitted, y_fitted, 'k', label='Fitted curve')
        ax.set_ylabel('Engagement')
        ax.set_xlabel('Time')
        ax.set_ylim(0)
        ax.set_xlim(0)
        ax.legend()
        plt.show()
        
        
    def train(self, df: pd.DataFrame): 
    
        fit_types = []
        parameter_list = []
    
        for i in tqdm(range(len(df))):
    
            x = df.loc[i, self.time_col]
            y = df.loc[i, "engagements"]
                
            self.fit(x=x, y=y)
    
            fit_types.append(self.best_fit)
            parameter_list.append(self.fits[self.best_fit]["params"])
            
        return fit_types, parameter_list

    
    def evaluate(self, train: pd.DataFrame, test: pd.DataFrame) -> float:
    
        prediction_count = 0
        score = 0
        
        for i in tqdm(range(len(train))):
            
            fit = train.loc[i, "fit_type"]
            params = train.loc[i, "parameters"]
            x = test.loc[i, self.time_col]
            y = test.loc[i, "engagements"]


            if x.size != 0: 
                # Some elements in test have no values because of the way the random mask was generated
                prediction_count += len(y)
                score += self.score(fit=fit, params=params, x=x, y=y)

            else:
                pass
    
        return score / prediction_count