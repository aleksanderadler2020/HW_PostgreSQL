import psycopg2


# Не знаю как в PostgreSQL обстоят дела с транзакциями, но открывать коннект делать десяток запросов
# и только потом закрывать, считаю нецелесообразно если нет массовой обработки чего нибудь,
# поэтому, открывать и закрывать коннект будем при каждом запросе.
def meta_query_execute(sql_text):
    with psycopg2.connect(database="HWDB", user="postgres", password="2451") as cn:
        with cn.cursor() as cur:
            cur.execute(sql_text)
        cn.commit()
    cn.close()


def write_query_execute(sql_text, params):
    with psycopg2.connect(database="HWDB", user="postgres", password="2451") as cn:
        with cn.cursor() as cur:
            cur.execute(sql_text, params)
        cn.commit()
    cn.close()


def read_query_execute(sql_text, params):
    with psycopg2.connect(database="HWDB", user="postgres", password="2451") as cn:
        with cn.cursor() as cur:
            cur.execute(sql_text, params)
            print('fetchall', cur.fetchall())
    cn.close()


def create_db():
    meta_query_execute("""DROP TABLE if exists clientphones;
                          DROP TABLE if exists clients;""")
    meta_query_execute("""CREATE TABLE public.clients (
                          id serial4 NOT NULL,
                          firstname varchar NOT NULL,
                          lastname varchar NOT NULL,
                          email varchar NULL,
                          CONSTRAINT clients_pk PRIMARY KEY (id));""")
    meta_query_execute("""CREATE TABLE public.clientphones (
                          id serial4 NOT NULL,
                          "owner" serial4 NOT NULL,
                          phone varchar NOT NULL,
                          CONSTRAINT clientphones_pk PRIMARY KEY (id),
                          CONSTRAINT clientphones_fk FOREIGN KEY ("owner") REFERENCES public.clients(id));""")


def add_client(first_name, last_name, email):
    write_query_execute("""INSERT INTO public.clients
                           (id, firstname, lastname, email)
                           VALUES(nextval('clients_id_seq'::regclass), %s, %s, %s);""",
                        (first_name, last_name, email))


def add_phone(client_id, phone):
    write_query_execute("""INSERT INTO public.clientphones
                           (id, "owner", phone)
                           VALUES(nextval('clientphones_id_seq'::regclass), %s, %s);""",
                        (client_id, phone))


def change_client(client_id, first_name=None, last_name=None, email=None):
    params = ()
    filters = ''
    if first_name is not None:
        params = params + (first_name,)
        filters = filters + ' firstname = %s'
    if last_name is not None:
        if filters != '':
            filters = filters + ','
        params = params + (last_name,)
        filters = filters + ' lastname = %s'
    if email is not None:
        if filters != '':
            filters = filters + ','
        params = params + (email,)
        filters = filters + ' email = %s'
    params = params + (client_id,)
    query = f'UPDATE public.clients SET {filters} WHERE id = %s;'
    write_query_execute(query, params)


def delete_phone(client_id, phone):
    write_query_execute('delete from public.clientphones a where a.owner = %s and a.phone = %s;', (client_id, phone))


def delete_client(client_id):
    write_query_execute('DELETE FROM public.clients WHERE id=%s;', (client_id,))


def find_client(first_name=None, last_name=None, email=None, phone=None):
    params = ()
    filters = ''
    query = 'select distinct cl.id, cl.firstname, cl.lastname, cl.email from public.clients cl'
    if first_name is not None:
        params = params + (first_name,)
        filters = filters + ' firstname = %s'
    if last_name is not None:
        if filters != '':
            filters = filters + 'and '
        params = params + (last_name,)
        filters = filters + ' lastname = %s'
    if email is not None:
        if filters != '':
            filters = filters + 'and '
        params = params + (email,)
        filters = filters + ' email = %s'
    if phone is not None:
        if filters != '':
            filters = filters + 'and '
        params = params + (phone,)
        query = query + ' inner join public.clientphones ph on ph."owner" = cl.id'
        filters = filters + ' phone = %s'
    if filters != '':
        query = query + ' where ' + filters
    read_query_execute(query, params)


if __name__ == "__main__":
    with psycopg2.connect(database="HWDB", user="postgres", password="2451") as conn:
        create_db()
        add_client('Иванов', 'Иван', 'mail.ru')
        add_phone(1, '+70001122334')
        add_phone(1, '+79991122334')
        change_client(1, 'Петров', 'Петр', 'gmail.ru')
        delete_phone(1, '+70001122334')
        add_client('Создаю', 'Чтобы', 'gmail.ru')
        find_client(phone='+79991122334')
        delete_client(2)

    conn.close()
