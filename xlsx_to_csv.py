import pandas as pd
import sys

# arg1 - source xlsx file
# arg2 - source xlsx sheet
# arg3 - destination csv
if __name__ == '__main__':
    df = pd.read_excel(sys.argv[1], sheet_name=sys.argv[2], header=0)
    df.to_csv(sys.argv[3], index=False)

