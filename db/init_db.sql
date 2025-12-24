-- Active: 1766120172902@@localhost@5433@project_db
CREATE TABLE wines (
	id SERIAL PRIMARY KEY, -- в wine_title есть повторения
	wine_title VARCHAR(255) NOT NULL,
	country VARCHAR(100),
	region VARCHAR(100),
	winery VARCHAR(255),
	rating DECIMAL(2, 1), -- измеряется по пятибалльной системе с точностью до 1 знака после запятой
	number_of_ratings INT,
	price DECIMAL(7, 2), -- max is 3410.79
	year_of_production INT,
	wine_type VARCHAR(10) -- max is len('sparkling') = 9
);
