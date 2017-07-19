import psycopg2

class LogAnalysis(Object):
    DBNAME = "news"

    def most_popular_articles(self, count):
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        sql =  """SELECT articles.title, COUNT(log.path)
        FROM log
        JOIN articles ON log.path = CONCAT('/article/', articles.slug)
        GROUP BY articles.title
        ORDER BY count
        DESC LIMIT {:d};""".format(count)
        c.execute(sql)
        articles = c.fetchall()
        db.close()
        return articles

    def author_view(self):
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        sql =  """SELECT authors.name, SUM(articleCount.count)
        FROM authors ,
            (SELECT articles.author, COUNT(log.path)
            FROM log
            JOIN articles ON log.path = CONCAT('/article/', articles.slug)
            GROUP BY articles.author) AS articleCount
        WHERE authors.id = articleCount.author
        GROUP BY authors.name
        DESC;"""
        c.execute(sql)
        authors = c.fetchall()
        db.close()
        return authors

    def daily_error(self):
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        sql =  """SELECT to_char(badCount.day, 'Mon DD, YYYY'),
        round(CAST(badCount.count::float / goodCount.count * 100 AS numeric), 2)
        AS "percent" FROM
        (SELECT date_trunc('day', time) AS "day", COUNT(*) FROM log
        WHERE status NOT LIKE '2%' GROUP BY day) AS badCount,
        (SELECT date_trunc('day', time) AS "day", COUNT(*) FROM log
        WHERE status LIKE '2%' GROUP BY "day") AS goodCount WHERE
        badCount.day = goodCount.day
        AND (badCount.count::float / goodCount.count * 100) > 1
        GROUP BY badCount.day, badCount.count, goodCount.count;"""
        c.execute(sql)
        errors = c.fetchall()
        db.close()
        return errors


test = LogAnalysis()
print test.most_popular_articles(3)

    """select articles.title, count(log.path)
    from log join articles on log.path = concat('/article/', articles.slug)
    group by articles.title order by count desc limit 3;"""

    """
    select authors.name, sum(articleCount.count) from authors , (select articles.author, count(log.path)
    from log join articles on log.path = concat('/article/', articles.slug)
    group by articles.author) as articleCount where authors.id = articleCount.author group by authors.name desc;
    """

    """
    select to_char(badCount.day, 'Mon DD, YYYY'), round(CAST(badCount.count::float / goodCount.count * 100 as numeric), 2) as "percent" from
    (select date_trunc('day', time) as "day", count(*) from log where status not like '2%' group by day) as badCount,
    (select date_trunc('day', time) as "day", count(*) from log where status like '2%' group by "day") as goodCount where
    badCount.day = goodCount.day and (badCount.count::float / goodCount.count * 100) > 1 group by badCount.day, badCount.count, goodCount.count;
    """
