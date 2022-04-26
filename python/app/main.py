import psycopg2, time, logging
import flask
from flask import jsonify, request, Flask

app = Flask(__name__)

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}


##########################################################
# DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user="postgres",
        password="postgres",
        host="localhost",
        port="5433",
        database="postgres"
    )

    return db


##########################################################
# ENDPOINTS
##########################################################


@app.route('/')
def landing_page():
    return """

    Hello World (Python)!  <br/>
    <br/>
    Check the sources for instructions on how to use the endpoints!<br/>
    <br/>
    BD 2022 Team<br/>
    <br/>
    """


#POST
@app.route('/dbproj/user', methods=['POST'])
def registaUtilizador():
    conn = db_connection()
    cur = conn.cursor()

    payload = request.get_json()
    print(payload) #Print de todos os parametros passados pelo .json

    try:
        if "username" in payload and "password" in payload and "contacto" in payload:
            username = payload["username"]
            password = payload["password"]
            contacto = payload["contacto"]

            if "email" in payload:
                email = payload["email"]
            else:
                email = "-"

            if "cc" in payload and "morada" in payload: #Comprador
                cc = payload["cc"]
                morada = payload["morada"]

                if "nif" in payload:
                    nif = payload["nif"]
                else:
                    nif = "-"

                querie = """INSERT INTO pessoa(username, password, contacto, email) VALUES(%s, %s, %s, %s);"""
                values = (username, password, contacto, email)

                cur.execute("BEGIN TRANSACTION;")
                cur.execute(querie, values)
                cur.execute("commit;")

                cur.execute("SELECT max(id) FROM pessoa;")
                rows = cur.fetchall()
                pessoa_id = int(rows[0][0])


                querie = """INSERT INTO comprador(cc, morada, nif, pessoa_id) VALUES(%s, %s, %s, %s);"""
                values = (cc, morada, nif, pessoa_id)

                cur.execute("BEGIN TRANSACTION;")
                cur.execute(querie, values)
                cur.execute("commit;")

                content = {'status': StatusCodes['success'], 'results': pessoa_id}
            elif "empresa" in payload and "nif" in payload and "morada" in payload: #Vendedor
                if "token" in payload:
                    token = payload["token"]

                    cur.execute("""SELECT count(*) FROM Administrador WHERE pessoa_id = %s;""", (token,))
                    rows = cur.fetchall()
                    count = int(rows[0][0])

                    if count == 0:
                        content = {'results': 'invalid'}
                    else:
                        empresa = payload["empresa"]
                        nif = payload["nif"]
                        morada = payload["morada"]

                        querie = """INSERT INTO pessoa(username, password, contacto, email) VALUES(%s, %s, %s, %s);"""
                        values = (username, password, contacto, email)

                        cur.execute("BEGIN TRANSACTION;")
                        cur.execute(querie, values)
                        cur.execute("commit;")

                        cur.execute("SELECT max(id) FROM pessoa;")
                        rows = cur.fetchall()
                        pessoa_id = int(rows[0][0])


                        querie = """INSERT INTO vendedor(empresa, nif, morada, pessoa_id) VALUES(%s, %s, %s, %s);"""
                        values = (empresa, nif, morada, pessoa_id)

                        cur.execute("BEGIN TRANSACTION;")
                        cur.execute(querie, values)
                        cur.execute("commit;")

                        content = {'status': StatusCodes['success'], 'results': pessoa_id}
                else:
                    content = {'results': 'invalid'}
            elif "area" in payload: #Administrador
                querie = """SELECT count(*) FROM Administrador;"""
                cur.execute(querie);

                rows = cur.fetchall()
                num = int(rows[0][0])
                print("Numero de admins: ", num);

                if num == 0:
                    area = payload["area"]

                    querie = """INSERT INTO pessoa(username, password, contacto, email) VALUES(%s, %s, %s, %s);"""
                    values = (username, password, contacto, email)

                    cur.execute("BEGIN TRANSACTION;")
                    cur.execute(querie, values)
                    cur.execute("commit;")

                    cur.execute("SELECT max(id) FROM pessoa;")
                    rows = cur.fetchall()
                    pessoa_id = int(rows[0][0])


                    querie = """INSERT INTO administrador(area, pessoa_id) VALUES(%s, %s);"""
                    values = (area, pessoa_id)

                    cur.execute("BEGIN TRANSACTION;")
                    cur.execute(querie, values)
                    cur.execute("commit;")

                    content = {'status': StatusCodes['success'], 'results': pessoa_id}
                else:
                    if "token" in payload:
                        token = payload["token"]

                        cur.execute("""SELECT count(*) FROM Administrador WHERE pessoa_id = %s;""", (token,))
                        rows = cur.fetchall()
                        count = int(rows[0][0])

                        if count == 0:
                            content = {'results': 'invalid'}
                        else:
                            area = payload["area"]

                            querie = """INSERT INTO pessoa(username, password, contacto, email) VALUES(%s, %s, %s, %s);"""
                            values = (username, password, contacto, email)

                            cur.execute("BEGIN TRANSACTION;")
                            cur.execute(querie, values)
                            cur.execute("commit;")

                            cur.execute("SELECT max(id) FROM pessoa;")
                            rows = cur.fetchall()
                            pessoa_id = int(rows[0][0])


                            querie = """INSERT INTO administrador(area, pessoa_id) VALUES(%s, %s);"""
                            values = (area, pessoa_id)

                            cur.execute("BEGIN TRANSACTION;")
                            cur.execute(querie, values)
                            cur.execute("commit;")

                            content = {'status': StatusCodes['success'], 'results': pessoa_id}
                    else:
                        content = {'results': 'invalid'}
            else:
                content = {'results': 'invalid'}
        else:
            content = {'results': 'invalid'}
    except (Exception, psycopg2.DatabaseError) as error:
        content = {'error:': str(error)}
    finally:
        if conn is not None:
            conn.close()

    return jsonify(content)

#POST
#TODO meter as cenas da versao
@app.route('/dbproj/produto', methods=['POST'])
def addProduto():
    conn = db_connection()
    cur = conn.cursor()

    payload = request.get_json()
    print(payload) #Print de todos os parametros passados pelo .json

    try:
        if "id" in payload and "marca" in payload and "stock" in payload and "vendedor_pessoa_id" in payload and "preco" in payload:
            id = payload["id"]
            marca = payload["marca"]
            stock = payload["stock"]
            vendedor_id = payload["vendedor_pessoa_id"]
            preco = payload["preco"]

            query = """ INSERT INTO 
                        produto(id, marca, stock, vendedor_pessoa_id)
                        VALUES
                        (%s, %s, %s, %s)"""
            values = (id, marca, stock, vendedor_id)

            if("descricao" in payload):
                descricao = payload["descricao"]
            else:
                descricao = "-"

            query3 = """INSERT INTO
                        versao(numero_versao, descricao, preco, data_alteracao, produto_id)
                        VALUES
                        (%s, %s, %s, TIMESTAMP, %s)"""
            values3 = (1, descricao, preco, id)
            cur.execute("BEGIN TRANSACTION")
            cur.execute(query, values)
            cur.execute(query3, values3)
            cur.execute("COMMIT")


            if("processador" in payload and "sistema_operativo" in payload and "armazenamento" in payload and "camara" in payload):

                processador = payload["processador"]
                sistema_operativo = payload["sistema_operativo"]
                armazenamento = payload["armazenamento"]
                camara = payload["camara"]

                query2 = """ INSERT INTO
                            computador(processador, sistema_operativo, armazenamento, camara, produto_id)
                            VALUES
                            (%s, %s, %s, %s)"""
                values2 = (processador, sistema_operativo, armazenamento, camara, id)

                cur.execute("BEGIN TRANSACTION")
                cur.execute(query2, values2)
                cur.execute("COMMIT")

                content = {'status': StatusCodes['success'], 'results': id}

            elif("comprimento" in payload and "largura" in payload and "peso" in payload and "resolucao" in payload):

                comprimento = payload["comprimento"]
                largura = payload["largura"]
                peso = payload["peso"]
                resolucao = payload["resolucao"]


                query2 = """INSERT INTO
                            televisor(comprimento, largura, peso, resolucao, produto_id)
                            VALUES
                            (%s, %s, %s, %s, %s)"""
                values2 = (comprimento, largura, peso, resolucao, id)

                cur.execute("BEGIN TRANSACTION")
                cur.execute(query2, values2)
                cur.execute("COMMIT")

                content = {'status': StatusCodes['success'], 'results': id}

            elif("modelo" in payload and "cor" in payload):

                modelo = payload["modelo"]
                cor = payload["cor"]

                query2 = """INSERT INTO
                            smartphone(modelo, cor, produto_id)
                            VALUES
                            (%s, %s, %s)"""
                values2 = (modelo, cor, id)

                cur.execute("BEGIN TRANSACTION")
                cur.execute(query2, values2)
                cur.execute("COMMIT")

                content = {'status': StatusCodes['success'], 'results': id}

            else:
                content = {'results': 'invalid'}
        else:
            content = {'results': 'invalid'}
    except (Exception, psycopg2.DatabaseError) as error:
        content = {'error:': str(error)}
    finally:
        if conn is not None:
            conn.close()

    return jsonify(content)

#PUT
@app.route('/dbproj/user', methods=['PUT'])
def autenticaUtilizador():
    conn = db_connection()
    cur = conn.cursor()

    payload = request.get_json()

    try:
        if "username" in payload and "password" in payload:
            username = payload["username"]
            password = payload["password"]

            querie = """SELECT id FROM pessoa WHERE pessoa.username = %s AND pessoa.password = %s;"""
            values = (username, password)

            cur.execute(querie, values)
            rows = cur.fetchall()

            if (len(rows) > 0):
                id = int(rows[0][0])
                content = {'token': id}
            else:
                content = {'results': 'there are not any person registered with this atributes'}
        else:
            content = {'results': 'invalid'}
    except (Exception, psycopg2.DatabaseError) as error:
        content = {'error:': str(error)}
    finally:
        if conn is not None:
            conn.close()

    return jsonify(content)


#MAIN
if __name__ == "__main__":
    time.sleep(1)  # just to let the DB start before this print :-)

    app.run(host="localhost", port=8080, debug=True, threaded=True)