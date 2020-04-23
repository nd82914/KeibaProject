import MySQLdb
import csv

def CreateTable():

    # MySQLの接続情報（各自の環境にあわせて設定のこと）
    db_config = {
    'host': 'localhost',
    'db': 'testdb',  # Database Name
    'user': 'public',
    'passwd': 'public',
    'charset': 'utf8',
    }

    try:    
        # 接続
        conn = MySQLdb.connect(host=db_config['host'], db=db_config['db'],\
        user=db_config['user'], passwd=db_config['passwd'], charset=db_config['charset'])
        #カーソル取得
        cur = conn.cursor()
    
        #main()
        with open("/mnt/c/Users/nd829/Downloads/RaceData_race_202009020205_.csv","r",encoding="utf_8",\
        errors="",newline="") as data:
            f = csv.reader(data, delimiter = ",", doublequote=True, lineterminator = "\r\n",\
            skipinitialspace=True)
            #header = next(f)
            #for row in f:
            #    print(row)
            heads=next(f)
            print(heads)
            #cur.execute("DROP TABLE IF EXISTS HORSE_DATA")
            cur.execute("""CREATE TABLE IF NOT EXISTS HORSE_DATA(
            Race_ID VARCHAR(20) NOT NULL COLLATE utf8mb4_unicode_ci, 
            Rank INT(3) NOT NULL, 
            Post_Number INT(3) NOT NULL,  
            Post_Position INT(3) NOT NULL, 
            Horse_Name VARCHAR(20) NOT NULL COLLATE utf8mb4_unicode_ci, 
            Sex_Age VARCHAR(3) NOT NULL COLLATE utf8mb4_unicode_ci,
            Weight INT(3) NOT NULL,
            Rider VARCHAR(8) NOT NULL COLLATE utf8mb4_unicode_ci,
            Time VARCHAR(8) NOT NULL COLLATE utf8mb4_unicode_ci,
            Margin VARCHAR(5) NOT NULL COLLATE utf8mb4_unicode_ci,
            Time_Index VARCHAR(5) NOT NULL COLLATE utf8mb4_unicode_ci,
            Passing VARCHAR(6) NOT NULL COLLATE utf8mb4_unicode_ci,
            3F VARCHAR(5) NOT NULL COLLATE utf8mb4_unicode_ci,
            Win FLOAT(7) NOT NULL,
            Favorite INT(3) NOT NULL,
            Horse_Weight VARCHAR(10) NOT NULL COLLATE utf8mb4_unicode_ci,
            Training_Time VARCHAR(8) NOT NULL COLLATE utf8mb4_unicode_ci,
            Comment VARCHAR(10) NOT NULL COLLATE utf8mb4_unicode_ci,
            Remark VARCHAR(10) NOT NULL COLLATE utf8mb4_unicode_ci,
            Trainer VARCHAR(10) NOT NULL COLLATE utf8mb4_unicode_ci,
            Owner VARCHAR(20) NOT NULL COLLATE utf8mb4_unicode_ci,
            Purse VARCHAR(10) NOT NULL COLLATE utf8mb4_unicode_ci,
            PRIMARY KEY (Race_ID, Horse_Name)
            )""")
            print("Create Table Successfully")
            #nextline=next(f)
            for row in f:
                #行追加　変数の埋め込みは変数が1つの場合最後のカンマをつけないとエラー
                #ON DUPLICATE KEY UPDATE Rank=VALUES(Rank) あればUPDATEする手もある、IGNOREはあればスキップ（エラーが出ない）
                cur.execute("""INSERT IGNORE INTO HORSE_DATA(Race_ID, Rank, Post_Number, Post_Position, Horse_Name, Sex_Age,
                Weight, Rider, Time, Margin, Time_Index, Passing, 3F, Win, Favorite, Horse_Weight, Training_Time,
                Comment, Remark, Trainer, Owner, Purse)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,(tuple(row))
                )

            #%(nextline[0],int(nextline[1]),int(nextline[2]),int(nextline[3]),nextline[4])
        #保存実行
        conn.commit()
        #接続を切る
        conn.close()

    except MySQLdb.Error as ex:
        print('MySQL Error: ', ex)



if __name__ == "__main__":
    CreateTable()
