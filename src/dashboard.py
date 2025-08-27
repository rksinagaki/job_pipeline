import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from collections import Counter
import boto3


try:
    df_filtered = pd.read_csv('s3://myproject-row-data1/filtered.csv')

    # -----------------
    # ダッシュボード化
    # -----------------
    st.title('データ職種求人ダッシュボード')
    st.sidebar.header('フィルタ')

    # -----------------
    # サイドバー
    # -----------------
    # 職種
    job_options = ['すべて','AIエンジニア','データサイエンティスト','データエンジニア','その他']
    selected_job = st.sidebar.selectbox('職種を選択:', options=job_options)

    # 勤務地
    all_areas = []
    for sublist in df_filtered['job_offer_areas'].dropna():
        for item in sublist:
            all_areas.append(item)
    unique_areas = list(set(all_areas))
    area_options = ['すべて'] + unique_areas
    selected_area = st.sidebar.selectbox('勤務地:', options=area_options)

    #給与
    with st.sidebar.expander("給与フィルター"):
        #最小値給与
        min_salary_options = ['すべて', '300万未満','300万～', '400万～', '500万～', '600万～', '700万～', '800万～', '900万～', '1000万～']
        selected_min_range = st.selectbox('最低年収:', options=min_salary_options)

        # 最大値給与
        max_salary_options = ['すべて', '300万未満','300万～', '400万～', '500万～', '600万～', '700万～', '800万～', '900万～', '1000万～']
        selected_max_range = st.selectbox('最高年収:', options=max_salary_options)

        # 代表値給与
        avg_salary_options = ['すべて', '300万未満','300万～', '400万～', '500万～', '600万～', '700万～', '800万～', '900万～', '1000万～']
        selected_avg_range = st.selectbox('年収の代表値):', options=avg_salary_options)

    # 従業員数
    employee_options = ['すべて', '〜100人', '101〜500人', '501〜1000人', '1001人〜']
    selected_emp_range = st.sidebar.selectbox('従業員数:', options=employee_options)

    # -----------------
    # フィルター
    # -----------------
    # 職種フィルタリング
    if selected_job != 'すべて':
        df_filtered_by_job = df_filtered[df_filtered['job_tag'] == selected_job]
    else:
        df_filtered_by_job = df_filtered

    # 勤務地フィルタリング
    if selected_area != 'すべて':
        df_filtered_by_area = df_filtered_by_job[df_filtered_by_job['job_offer_areas'].apply(
            # 不明
            lambda x: any(item in selected_area for item in x)
        )]
    else:
        df_filtered_by_area = df_filtered_by_job

    # 給与フィルタリング
    salary_filters = {
        'すべて': None,
        '300万未満': 300,
        '300万～': 300,
        '400万～': 400,
        '500万～': 500,
        '600万～': 600,
        '700万～': 700,
        '800万～': 800,
        '900万～': 900,
        '1000万～': 1000,
    }
    # 最小値フィルタ
    selected_min_salary_value = salary_filters[selected_min_range]
    if selected_min_salary_value is None:
        df_filtered_by_min = df_filtered_by_area
    elif selected_min_range == '300万未満':
        df_filtered_by_min = df_filtered_by_area[df_filtered_by_area['job_offer_min_salary'] < selected_min_salary_value]
    else:
        df_filtered_by_min = df_filtered_by_area[df_filtered_by_area['job_offer_min_salary'] >= selected_min_salary_value]

    # 最大値フィルタ
    selected_max_salary_value = salary_filters[selected_max_range]
    if selected_max_salary_value is None:
        df_filtered_by_max = df_filtered_by_min
    elif selected_max_range == '300万未満':
        df_filtered_by_max = df_filtered_by_min[df_filtered_by_min['job_offer_max_salary'] < selected_max_salary_value]
    else:
        df_filtered_by_max = df_filtered_by_min[df_filtered_by_min['job_offer_max_salary'] >= selected_max_salary_value]

    # 代表値フィルタ
    selected_avg_salary_value = salary_filters[selected_avg_range]
    if selected_avg_salary_value is None:
        df_filtered_by_avg = df_filtered_by_max
    elif selected_avg_range == '300万未満':
        df_filtered_by_avg = df_filtered_by_max[df_filtered_by_max['avg_salary'] < selected_avg_salary_value]
    else:
        df_filtered_by_avg = df_filtered_by_max[df_filtered_by_max['avg_salary'] >= selected_avg_salary_value]

    # 従業員数フィルタリング
    if selected_emp_range == '〜100人':
        df_filtered_final = df_filtered_by_avg[df_filtered_by_avg['employee_count'] <= 100]
    elif selected_emp_range == '101〜500人':
        df_filtered_final = df_filtered_by_avg[(df_filtered_by_avg['employee_count'] > 100) & (df_filtered_by_avg['employee_count'] <= 500)]
    elif selected_emp_range == '501〜1000人':
        df_filtered_final = df_filtered_by_avg[(df_filtered_by_avg['employee_count'] > 500) & (df_filtered_by_avg['employee_count'] <= 1000)]
    elif selected_emp_range == '1001人〜':
        df_filtered_final = df_filtered_by_avg[df_filtered_by_avg['employee_count'] > 1000]
    else:
        df_filtered_final = df_filtered_by_avg

    st.write(f"合計 {df_filtered_final['job_offer_id'].count()} 件の求人情報")
    st.dataframe(df_filtered_final)


    # -----------------
    # プロットする情報
    # -----------------
    # 職種別の求人割合
    job_counts = df_filtered_final['job_tag'].value_counts()
    fig_job_bar = px.bar(
        job_counts,
        x=job_counts.index,
        y=job_counts.values,
        title='職種別求人割合',
        labels={'x': '職種', 'y': '求人件数'}
    )
    st.plotly_chart(fig_job_bar)

    job_proportions = (df_filtered_final['job_tag'].value_counts()) / (df_filtered_final['job_tag'].value_counts().sum())*100
    st.markdown("* 職種別求人割合(表)")
    st.dataframe(job_proportions.round(1))

    # 職種ごとのスキルヒートマップ
    all_skills = [skill for sublist in df_filtered_final['job_offer_skill_names'].dropna() for skill in sublist]
    skill_counts = Counter(all_skills)
    top_skills = [skill for skill, count in skill_counts.most_common(20)]

    skill_matrix = pd.DataFrame(index=top_skills, columns=df_filtered_final['job_tag'].unique()).fillna(0)
    for job_tag, skills in zip(df_filtered_final['job_tag'], df_filtered_final['job_offer_skill_names']):
        if isinstance(skills, list):
            for skill in skills:
                if skill in skill_matrix.index:
                    skill_matrix.loc[skill, job_tag] += 1

    fig_heatmap = px.imshow(
        skill_matrix,
        x=skill_matrix.columns,
        y=skill_matrix.index,
        color_continuous_scale='YlGnBu',
        labels=dict(x="職種", y="スキル", color="件数"),
        title="職種別スキルヒートマップ"
    )
    fig_heatmap.update_layout(
        height= 800,
        width=1200
    )

    st.plotly_chart(fig_heatmap)

    # スキル表
    df_skill_counts = pd.DataFrame(
        skill_counts.most_common(),
        columns=['スキル名', '求人件数']
    )

    st.subheader("スキル別求人件数")
    st.dataframe(df_skill_counts)

    # 職種ごとの平均年収
    jobs = ['AIエンジニア', 'データサイエンティスト', 'データエンジニア']
    for job in jobs:
        df_job = df_filtered_final[df_filtered_final['job_tag'] == job]
        df_job = df_job[df_job['avg_salary'] >= 100]

        fig = px.histogram(
            df_job,
            x='avg_salary',
            title=f'{job}の平均年収分布',
            labels={'avg_salary': '平均年収（万円）', 'count': '求人件数'}
        )
        st.plotly_chart(fig)

    # 給与の中央値（箱ひげ図）
    st.subheader('代表値の分布')

    fig_salary = px.box(
        df_filtered_final,
        x='job_tag',
        y='avg_salary',
        points='all',
        title='職種別給与分布'
    )
    st.plotly_chart(fig_salary)

    # 給与の最低値（箱ひげ図）
    st.subheader('最低値の分布')

    fig_salary = px.box(
        df_filtered_final,
        x='job_tag',
        y='job_offer_min_salary',
        points='all',
        title='職種別給与分布'
    )
    st.plotly_chart(fig_salary)

    # 給与の最大値（箱ひげ図）
    st.subheader('最大値の分布')

    fig_salary = px.box(
        df_filtered_final,
        x='job_tag',
        y='job_offer_max_salary',
        points='all',
        title='職種別給与分布'
    )
    st.plotly_chart(fig_salary)

    # 企業の規模（従業員数）の分布
    st.subheader('企業の規模')

    fig_emp = px.histogram(df_filtered_final, x='employee_count', title='従業員数の分布（対数スケール）',nbins=20)
    fig_emp = px.histogram(
        df_filtered_final,
        x='employee_count',
        nbins=20,
        title='従業員数の分布'
    )
    st.plotly_chart(fig_emp)

    # 求人棒グラフ
    # 他の求人サイトが大体下記のような従業員での分け方だったので、それを参考に決定
    bins = [0, 50, 100, 500, 1000, 5000, 10000, float('inf')]
    labels = ['~50人','51~100人', '101~500人', '501~1000人', '1001~5000人', '5001~10000人', '10001人~']

    df_filtered_final['employee_category'] = pd.cut(
        df_filtered_final['employee_count'],
        bins=bins,
        labels=labels
    )

    employee_counts = df_filtered_final['employee_category'].value_counts().reindex(labels)

    fig_employee_bar = px.bar(
        employee_counts,
        x=employee_counts.index,
        y=employee_counts.values,
        title='企業規模別の求人件数',
        labels={'x': '企業規模（従業員数）', 'y': '求人件数'}
    )

    st.plotly_chart(fig_employee_bar)

except Exception as e:
    st.error(f"エラーが発生しました: {e}")
    print(f"DEBUG: エラーが発生しました: {e}")