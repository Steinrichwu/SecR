import numpy as np
import scipy.optimize as solver
import matplotlib.pyplot as plt

#Pre-processing
weight=np.array([[0.2,0.2,0.2,0.2,0.2]])

# the cov_matrix among stocks
cov_matrix=np.array([[0.004880084 ,0.001154875,0.000805328 ,-0.001046223 ,0.006391943],
                     [0.001154875, 0.001737042, 0.00057684, -0.000903226, 0.005952862],
                     [0.000805328, 0.00057684 ,0.003003591, 0.000430984, 0.002308259],
                     [-0.001046223, -0.000903226, 0.000430984, 0.023605895 ,-0.002008894],
                     [0.006391943,0.005952862,0.002308259,-0.002008894,0.037472509]])

#Calculate variance of portfolio:
def calculate_portfolio_var(w,V):
    w=np.array(w)
    return (np.dot(np.dot(w,V),w.T))

#Calculate position risk(mrc->trc)
def mrc(weight,cov_matrix):
    mrc = (weight@cov_matrix).T
    return mrc

def trc(weight,cov_matrix):
    trc = mrc(weight,cov_matrix)*(weight.T)
    return trc

#Calculate Total risk
def portrisk(weight,cov_matrix):
    port_risk = trc(weight,cov_matrix).sum()
    return port_risk

#Calculate risk contribution
def risk_contribution(weight,cov_matrix):
    risk_cont = trc(weight,cov_matrix)/portrisk(weight,cov_matrix)
    return risk_cont

#Establish the optimization objective
def risk_budget_objective(x,pars):
    V = pars[0]
    x_t = pars[1]
    sig_p =calculate_portfolio_var(x,V)
    risk_target = np.asmatrix(np.multiply(sig_p,x_t))
    asset_RC =trc(x,V)
    Error = (np.square(asset_RC-risk_target)).sum()
    return Error




#Optimization
'''
Constraints
1.Sum of weights = 100%
2.Risk Contribution all equal
3.weight > 0
'''
def total_weight_constraint(x):
    return np.sum(x)-1.0

def long_only_constraint(x):
    return x



def calcu_w(x):
    '''
    :param x: Expected contribution weight of each part
    :return: corresponding weight of each stocks
    '''
    w0 = [0.1,0.3,0.2,0.2,0.2]     #w0为迭代时初始权重
    x_t = x                        #x_t为希望分配给每支股票的权重
    cons = ({'type':'eq','fun':total_weight_constraint},   #'eq':使total_weight_constraint 这个function return 的数 = 0
            {'type':'ineq','fun':long_only_constraint})    #'ineq':使long_only_constraint 这个function return的数 > 0
    options={'disp':True,'maxiter':1000,'ftol':1e-20}     #设定精度（ftol）和迭代次数（maxiter）
    res=solver.minimize(risk_budget_objective,x0=w0,constraints=cons,args=[cov_matrix,x_t],options=options,method='SLSQP')
    # args为输入的risk_budget_objective的值
    # method有不同方法可以选择
    outcome=res.x
    return outcome
#wb = calcu_w([0.2,0.2,0.2,0.2,0.2])

# def simulation(wb):
#     total=[]
#     for i in range(10000):
#         w = np.random.rand(5)
#         w/= sum(w)
#         total+=[np.sqrt(calculate_portfolio_var(w,cov_matrix))]
#
#     x=np.linspace(0,0.25,1000)
#     count=np.zeros((1,1000))
#
#     for i in range(10000):
#         for j in range(1000):
#             if x[j]<=total[i]<x[j+1]:
#                 count[0,j]+=1
#             if x[j]<=np.sqrt(calculate_portfolio_var(wb,cov_matrix))<x[j+1]:
#                 percent = j
#     per = 0
#     for i in range(percent):
#         per += count[0,i]
#     lab = per/1000*100
#
#     plt.scatter(x,count)
#     plt.vlines(np.sqrt(calculate_portfolio_var(wb,cov_matrix)),0,70,colors='r',linestyles='--',label='Our Portfolio: in '+str(lab.round(0))+'% percentile')
#     plt.legend()
#     plt.show()
# simulation(wb)
