all_files = os.listdir(folder_path)
txt_files = [os.path.join(folder_path, f) for f in all_files if f.endswith('.txt')]
dfs = []
for file in txt_files:
    df = pd.read_csv(file, delimiter="\t", skiprows=2, dtype=dtype_dict)
    df = df.iloc[:-3,2:]
    df.columns = df.columns.str.strip()
    df['Pstng Date'] = pd.to_datetime(df['Pstng Date'], format='%m/%d/%Y')