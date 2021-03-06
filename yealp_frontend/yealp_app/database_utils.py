from flask import g
def get_user(user_id):
    user = g.conn.execute('SELECT * FROM USERS WHERE user_id = %s', (user_id, )).fetchone()
    return user

def get_restaurant(business_id):
    restaurant = g.conn.execute('SELECT * FROM business WHERE business_id = %s', (business_id, )).fetchone()
    return restaurant


def get_detailed_reviews_with_user(business_id):
    reviews = g.conn.execute('''
        WITH one_restaurant AS (
            SELECT *
            FROM Review_of_Business
            WHERE business_id = %s AND detailed_review IS NOT NULL)
        SELECT review_id, Users.name as username, short_tip, stars, user_id,
               detailed_review, review_date,
               useful, funny, cool	
        FROM one_restaurant
            LEFT JOIN Users_write_Review USING(review_id)
            LEFT JOIN Users USING(user_id)
        ORDER BY review_date DESC''', (business_id, )).fetchall()
    return reviews

def get_filter_business():
    restaurants = g.conn.execute('''
    With bizs_id_in_one_state as(
                    SELECT *
                    FROM Business
                    where state = %s
                ),
                bizs_in_one_state as(
                    SELECT business_id, round(AVG(stars), 2) AS average_stars
                    FROM Review_of_Business JOIN bizs_id_in_one_state USING(business_id)
                    WHERE detailed_review IS NOT NULL
                    GROUP BY business_id)
                SELECT * 
                FROM bizs_in_one_state RIGHT JOIN bizs_id_in_one_state USING(business_id)
    ''')