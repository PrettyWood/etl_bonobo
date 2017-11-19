import pandas as pd


def out_lines1(dfs):
    df = dfs['lines1']
    return {'aa': df[df.breakdown == 'Day']}


def out_lines2(dfs):
    df1, df2 = dfs['lines1'], dfs['lines2']
    return {'aaa': df1[df1.breakdown == 'YTD'],
            'bbb': df2[df2.breakdown == 'YTD']}


def out_lines3(dfs):
    df = dfs['lines1']
    return {'zzzz': df[df.breakdown == 'YTD']}


def out_lines4(dfs):
    df = dfs['lines2']
    return {'ygtd': df[df.value >= 9]}
