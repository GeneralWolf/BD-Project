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

#TODO cant fucking use produto if inserting another table, can I?
#TODO wait no i kinda can i believe
#TODO still not sure if the endpoint should be produto
@app.route('/produto/', methods=['POST'])
def add_produto():
    logger.info('POST /produto')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /produto - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'produto_id' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'produto_id value not in payload'}
        return flask.jsonify(response)

    #TODO check if better way to do this
    #TODO update description and price on table versao
    if(payload['tipo'] == 'computador'):
        comp_create_stmt = """INSERT INTO computador
                    (processador, sistema_operativo, armazenamento, camara, produto_id, produto_marca, produto_stock, produto_vendedor)
                   VALUES
                    (%s, %s, %s, %s, %d, %s, %d, %d)"""
        values_comp = (payload['processador'], payload['sistema_operativo'], payload['armazenamento'], payload['camara'], payload['produto_id'], payload['produto_marca'], payload['produto_stock'], payload['produto_vendedor'])

        #TODO trying to also add the versao table part
        #TODO also why is the version product id still have smartphone in the name???
        version_create_stmt = """INSERT INTO versao
                            (descricao, preco, data_alteracao, smartphone_produto_id)
                            VALUES
                            (%s, %f, TIMESTAMP, %d)
                                """
        values_versao = (payload['descricao'], payload['preco'], payload['produto_id'])

        try:
            cur.execute(comp_create_stmt, values_comp)
            cur.execute(version_create_stmt, values_versao)

            # commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': f'Inserted produto (computador) {payload["id_produto"]}'}

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f'POST /produto - error: {error}')
            response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

            # an error occurred, rollback
            conn.rollback()

        finally:
            if conn is not None:
                conn.close()

    elif(payload['tipo'] == 'televisor'):
        tel_create_stmt = """INSERT INTO televisor
                    (comprimento, largura, peso, resolucao, produto_id, produto_marca, produto_stock, produto_vendedor)
                   VALUES
                    (%d, %d, %d, %s, %d, %s, %d, %d)"""
        values_tel = (payload['comprimento'], payload['largura'], payload['peso'], payload['resolucao'], payload['produto_id'], payload['produto_marca'], payload['produto_stock'], payload['produto_vendedor'])

        version_create_stmt = """INSERT INTO versao
                                    (descricao, preco, data_alteracao, smartphone_produto_id)
                                    VALUES
                                    (%s, %f, TIMESTAMP, %d)
                                        """
        values_versao = (payload['descricao'], payload['preco'], payload['produto_id'])

        try:
            cur.execute(tel_create_stmt, values_tel)
            cur.execute(version_create_stmt, values_versao)

            # commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': f'Inserted produto (televisor) {payload["id_produto"]}'}

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f'POST /produto - error: {error}')
            response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

            # an error occurred, rollback
            conn.rollback()

        finally:
            if conn is not None:
                conn.close()

    elif (payload['tipo'] == 'smartphone'):
        phone_create_stmt = """INSERT INTO smartphone
                        (modelo, cor, produto_id, produto_marca, produto_stock, produto_vendedor)
                       VALUES
                        (%s, %s, %d, %s, %d, %d)"""
        values_phone = (payload['modelo'], payload['cor'], payload['produto_id'], payload['produto_marca'], payload['produto_stock'], payload['produto_vendedor'])

        version_create_stmt = """INSERT INTO versao
                                    (descricao, preco, data_alteracao, smartphone_produto_id)
                                    VALUES
                                    (%s, %f, TIMESTAMP, %d)
                                        """
        values_versao = (payload['descricao'], payload['preco'], payload['produto_id'])

        try:
            cur.execute(phone_create_stmt, values_phone)
            cur.execute(version_create_stmt, values_versao)

            # commit the transaction
            conn.commit()
            response = {'status': StatusCodes['success'], 'results': f'Inserted produto (smartphone) {payload["id_produto"]}'}

        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(f'POST /produto - error: {error}')
            response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

            # an error occurred, rollback
            conn.rollback()

        finally:
            if conn is not None:
                conn.close()


    return flask.jsonify(response)


#MAIN
if __name__ == "__main__":
    time.sleep(1)  # just to let the DB start before this print :-)

    app.run(host="localhost", port=8080, debug=True, threaded=True)