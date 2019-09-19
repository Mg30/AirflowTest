from settings import CONN, CREATE_STATEMENTS

def main():
    cur = CONN.connect()
    [cur.execute(statement)for statement in CREATE_STATEMENTS]

if __name__ == "__main__":
    main()