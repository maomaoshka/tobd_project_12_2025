import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
from datetime import datetime

# Настройка страницы
st.set_page_config(
    page_title="Wine Analytics",
    page_icon="🍷",
    layout="wide"
)

# Функция для выполнения SQL запросов
@st.cache_data(ttl=300)  # Кешируем на 5 минут
def execute_query(query, params=None, return_df=True):
    """
    Выполняет SQL запрос и возвращает результат
    """
    try:
        # Параметры подключения
        db_params = {
            'host': 'localhost',
            'port': '5433',
            'database': 'project_db',
            'user': 'postgres',
            'password': 'password'
        }
        
        # Устанавливаем соединение
        connection = psycopg2.connect(**db_params)
        
        if return_df:
            # Для SELECT запросов возвращаем DataFrame
            df = pd.read_sql_query(query, connection, params=params)
            connection.close()
            return df
        else:
            # Для других операций (INSERT, UPDATE и т.д.)
            cursor = connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=colnames)
                cursor.close()
                connection.close()
                return df
            else:
                connection.commit()
                affected_rows = cursor.rowcount
                cursor.close()
                connection.close()
                return affected_rows
                
    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return None

# Заголовок приложения
st.title("🍷 Аналитика вин")
st.markdown("---")

# Сайдбар с информацией
with st.sidebar:
    st.header("ℹ️ О приложении")
    st.write("""
    Это дашборд для анализа базы данных вин.
    Все запросы выполняются напрямую к PostgreSQL.
    """)
    
    # Информация о последнем обновлении
    st.markdown(f"**Последнее обновление:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Кнопка обновления данных
    if st.button("🔄 Обновить данные"):
        st.cache_data.clear()

# Раздел 1: Основные метрики
st.header("📊 Основные показатели")

# Создаем 4 колонки для метрик
col1, col2, col3, col4 = st.columns(4)

# Метрика 1: Общее количество вин
with col1:
    query_total = "SELECT COUNT(*) as total_wines FROM wines;"
    df_total = execute_query(query_total)
    if df_total is not None:
        total_wines = df_total['total_wines'].iloc[0]
        st.metric(
            label="Всего вин",
            value=f"{total_wines:,}",
            help="Общее количество вин в базе данных"
        )
    else:
        st.metric(label="Всего вин", value="N/A")

# Метрика 2: Средняя цена
with col2:
    query_avg_price = "SELECT AVG(price) as avg_price FROM wines WHERE price IS NOT NULL;"
    df_avg_price = execute_query(query_avg_price)
    if df_avg_price is not None and not df_avg_price.empty:
        avg_price = df_avg_price['avg_price'].iloc[0]
        st.metric(
            label="Средняя цена",
            value=f"${avg_price:,.2f}",
            help="Средняя цена вина в долларах"
        )
    else:
        st.metric(label="Средняя цена", value="N/A")

# Метрика 3: Средний рейтинг
with col3:
    query_avg_rating = "SELECT AVG(rating) as avg_rating FROM wines WHERE rating IS NOT NULL;"
    df_avg_rating = execute_query(query_avg_rating)
    if df_avg_rating is not None and not df_avg_rating.empty:
        avg_rating = df_avg_rating['avg_rating'].iloc[0]
        st.metric(
            label="Средний рейтинг",
            value=f"{avg_rating:.2f}",
            help="Средний рейтинг вин по 5-балльной шкале"
        )
    else:
        st.metric(label="Средний рейтинг", value="N/A")

# Метрика 4: Количество стран
with col4:
    query_countries = "SELECT COUNT(DISTINCT country) as unique_countries FROM wines WHERE country IS NOT NULL;"
    df_countries = execute_query(query_countries)
    if df_countries is not None and not df_countries.empty:
        unique_countries = df_countries['unique_countries'].iloc[0]
        st.metric(
            label="Стран",
            value=unique_countries,
            help="Количество уникальных стран производства"
        )
    else:
        st.metric(label="Стран", value="N/A")

st.markdown("---")

# Раздел 2: Простая визуализация - топ стран
st.header("🌍 Топ-10 стран по количеству вин")

# Запрос для получения топ-10 стран
query_top_countries = """
SELECT 
    country,
    COUNT(*) as wine_count,
    AVG(price) as avg_price,
    AVG(rating) as avg_rating
FROM wines 
WHERE country IS NOT NULL 
GROUP BY country 
ORDER BY wine_count DESC 
LIMIT 10;
"""

df_top_countries = execute_query(query_top_countries)

if df_top_countries is not None and not df_top_countries.empty:
    # Создаем столбчатую диаграмму
    fig = px.bar(
        df_top_countries,
        x='country',
        y='wine_count',
        title="Количество вин по странам",
        labels={'country': 'Страна', 'wine_count': 'Количество вин'},
        color='wine_count',
        color_continuous_scale='Reds'
    )
    
    # Настраиваем отображение
    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis_title="Количество вин",
        coloraxis_showscale=False
    )
    
    # Показываем график
    st.plotly_chart(fig, use_container_width=True)
    
    # Показываем таблицу под графиком
    with st.expander("📋 Показать данные таблицей"):
        st.dataframe(
            df_top_countries,
            column_config={
                "country": "Страна",
                "wine_count": "Количество вин",
                "avg_price": st.column_config.NumberColumn(
                    "Средняя цена",
                    format="$%.2f"
                ),
                "avg_rating": st.column_config.NumberColumn(
                    "Средний рейтинг",
                    format="%.2f"
                )
            },
            hide_index=True
        )
else:
    st.info("Нет данных для отображения")

st.markdown("---")

# Раздел 3: Распределение по типам вин
st.header("🍇 Распределение по типам вин")

# Запрос для получения распределения по типам
query_wine_types = """
SELECT 
    wine_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM wines 
WHERE wine_type IS NOT NULL 
GROUP BY wine_type 
ORDER BY count DESC;
"""

df_wine_types = execute_query(query_wine_types)

if df_wine_types is not None and not df_wine_types.empty:
    # Создаем две колонки: график и таблица
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Круговая диаграмма
        fig2 = px.pie(
            df_wine_types,
            values='count',
            names='wine_type',
            title="Процентное соотношение типов вин",
            hole=0.3,
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        # Таблица с процентами
        st.write("**Статистика по типам:**")
        for _, row in df_wine_types.iterrows():
            st.markdown(f"**{row['wine_type']}:** {row['count']} ({row['percentage']}%)")

st.markdown("---")

# Раздел 5: Анализ по годам производства
st.header("📅 Динамика по годам")

# Запрос для анализа по годам
query_years = """
SELECT 
    year_of_production,
    COUNT(*) as wine_count,
    AVG(price) as avg_price,
    AVG(rating) as avg_rating
FROM wines 
WHERE year_of_production IS NOT NULL 
    AND year_of_production >= 2000
GROUP BY year_of_production
HAVING COUNT(*) >= 10
ORDER BY year_of_production;
"""

df_years = execute_query(query_years)

if df_years is not None and not df_years.empty:
    # Создаем вкладки для разных графиков
    tab1, tab2, tab3 = st.tabs(["📈 Количество вин", "💰 Цены", "⭐ Рейтинги"])
    
    with tab1:
        fig_count = px.line(
            df_years,
            x='year_of_production',
            y='wine_count',
            markers=True,
            title="Количество вин по годам производства",
            labels={'year_of_production': 'Год', 'wine_count': 'Количество вин'}
        )
        fig_count.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_count, use_container_width=True)
    
    with tab2:
        fig_price = px.line(
            df_years,
            x='year_of_production',
            y='avg_price',
            markers=True,
            title="Средняя цена по годам производства",
            labels={'year_of_production': 'Год', 'avg_price': 'Средняя цена ($)'}
        )
        fig_price.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_price, use_container_width=True)
        
        # Рассчитываем изменение цены
        if len(df_years) > 1:
            first_price = df_years['avg_price'].iloc[0]
            last_price = df_years['avg_price'].iloc[-1]
            price_change = ((last_price - first_price) / first_price) * 100
            st.metric(
                "Изменение средней цены за период",
                f"${last_price:.2f}",
                f"{price_change:+.1f}%"
            )
    
    with tab3:
        fig_rating = px.line(
            df_years,
            x='year_of_production',
            y='avg_rating',
            markers=True,
            title="Средний рейтинг по годам производства",
            labels={'year_of_production': 'Год', 'avg_rating': 'Средний рейтинг'}
        )
        fig_rating.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_rating, use_container_width=True)
    
    with st.expander("📋 Статистика по годам"):
        st.dataframe(
            df_years,
            column_config={
                "year_of_production": "Год",
                "wine_count": "Количество вин",
                "avg_price": st.column_config.NumberColumn(
                    "Средняя цена",
                    format="$%.2f"
                ),
                "avg_rating": st.column_config.NumberColumn(
                    "Средний рейтинг",
                    format="%.2f"
                )
            },
            hide_index=True
        )
else:
    st.info("Недостаточно данных для анализа по годам")

st.markdown("---")

# Раздел 6: Топ виноделен
st.header("🏆 Лучшие винодельни")

# Запрос для топ виноделен
query_wineries = """
SELECT 
    winery,
    COUNT(*) as wine_count,
    AVG(rating) as avg_rating,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price
FROM wines 
WHERE winery IS NOT NULL 
    AND rating IS NOT NULL
GROUP BY winery
HAVING COUNT(*) >= 3  -- только винодельни с 3+ винами
ORDER BY avg_rating DESC 
LIMIT 10;
"""

df_wineries = execute_query(query_wineries)

if df_wineries is not None and not df_wineries.empty:
    # Выбор критерия сортировки
    sort_by = st.selectbox(
        "Сортировать винодельни по:",
        ["avg_rating", "wine_count", "avg_price"],
        format_func=lambda x: {
            "avg_rating": "Среднему рейтингу",
            "wine_count": "Количеству вин",
            "avg_price": "Средней цене"
        }[x]
    )
    
    # Сортируем по выбранному критерию
    df_sorted = df_wineries.sort_values(by=sort_by, ascending=(sort_by == "avg_price"))
    
    # Столбчатая диаграмма
    fig_wineries = px.bar(
        df_sorted.head(10),
        x='winery',
        y=sort_by,
        color=sort_by,
        color_continuous_scale='Viridis',
        labels={
            'winery': 'Винодельня',
            'avg_rating': 'Средний рейтинг',
            'wine_count': 'Количество вин',
            'avg_price': 'Средняя цена ($)'
        },
        title=f"Топ-10 виноделен по {'среднему рейтингу' if sort_by == 'avg_rating' else 'количеству вин' if sort_by == 'wine_count' else 'средней цене'}"
    )
    
    fig_wineries.update_layout(
        xaxis_tickangle=-45,
        coloraxis_showscale=False
    )
    
    st.plotly_chart(fig_wineries, use_container_width=True)
    
    # Таца с деталями
    with st.expander("📋 Детали по винодельням"):
        st.dataframe(
            df_sorted,
            column_config={
                "winery": "Винодельня",
                "wine_count": "Кол-во вин",
                "avg_rating": st.column_config.NumberColumn(
                    "Ср. рейтинг",
                    format="%.2f"
                ),
                "avg_price": st.column_config.NumberColumn(
                    "Ср. цена",
                    format="$%.2f"
                ),
                "min_price": st.column_config.NumberColumn(
                    "Мин. цена",
                    format="$%.2f"
                ),
                "max_price": st.column_config.NumberColumn(
                    "Макс. цена",
                    format="$%.2f"
                )
            },
            hide_index=True
        )
else:
    st.info("Недостаточно данных о винодельнях")

st.markdown("---")

# Раздел 5: Динамические фильтры (демо)
st.header("🔍 Быстрые фильтры")

# Создаем колонки для фильтров
filter_col1, filter_col2, filter_col3 = st.columns(3)

with filter_col1:
    # Фильтр по минимальному рейтингу
    min_rating = st.slider(
        "Минимальный рейтинг",
        min_value=0.0,
        max_value=5.0,
        value=3.5,
        step=0.1,
        help="Показать вина с рейтингом выше указанного"
    )

with filter_col2:
    # Фильтр по максимальной цене
    max_price = st.slider(
        "Максимальная цена ($)",
        min_value=0,
        max_value=500,
        value=100,
        step=5,
        help="Показать вина дешевле указанной цены"
    )

with filter_col3:
    # Выбор типа вина
    wine_type_options_query = "SELECT DISTINCT wine_type FROM wines WHERE wine_type IS NOT NULL ORDER BY wine_type;"
    df_wine_types_options = execute_query(wine_type_options_query)
    
    if df_wine_types_options is not None:
        wine_types_list = df_wine_types_options['wine_type'].tolist()
        selected_type = st.selectbox(
            "Тип вина",
            options=["Все"] + wine_types_list,
            help="Выберите тип вина"
        )

# Кнопка применения фильтров
if st.button("Применить фильтры", type="primary"):
    # Формируем запрос с фильтрами
    query_filtered = """
    SELECT 
        wine_title,
        country,
        winery,
        rating,
        price,
        wine_type
    FROM wines 
    WHERE 1=1
    """
    
    params = []
    
    # Добавляем условия фильтрации
    if min_rating > 0:
        query_filtered += " AND rating >= %s"
        params.append(min_rating)
    
    if max_price < 500:
        query_filtered += " AND price <= %s"
        params.append(max_price)
    
    if selected_type != "Все":
        query_filtered += " AND wine_type = %s"
        params.append(selected_type)
    
    query_filtered += " LIMIT 10;"
    
    # Выполняем запрос
    df_filtered = execute_query(query_filtered, params=params)
    
    if df_filtered is not None and not df_filtered.empty:
        st.success(f"Найдено {len(df_filtered)} вин по вашему запросу:")
        st.dataframe(
            df_filtered,
            column_config={
                "wine_title": "Название",
                "country": "Страна",
                "winery": "Винодельня",
                "rating": "Рейтинг",
                "price": st.column_config.NumberColumn("Цена", format="$%.2f"),
                "wine_type": "Тип"
            },
            hide_index=True
        )
    else:
        st.warning("По вашему запросу ничего не найдено")

# Футер
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 0.9rem;'>
        Wine Analytics Dashboard • PostgreSQL + Streamlit • 
        <span id='datetime'></span>
    </div>
    
    <script>
        function updateDateTime() {
            const now = new Date();
            const options = { 
                year: 'numeric', 
                month: 'long', 
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            };
            document.getElementById('datetime').textContent = 
                'Текущее время: ' + now.toLocaleDateString('ru-RU', options);
        }
        updateDateTime();
        setInterval(updateDateTime, 1000);
    </script>
    """,
    unsafe_allow_html=True
)