import pandas as pd
import numpy as np
np.random.seed(1492)
test_df = pd.DataFrame({"var1":np.random.randn(5000)})
test_df.hist()
