import psycopg2, time
import flask
from flask import jsonify, request, Flask
import logging

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

#3 ponto
#POST
#TODO test
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

            if ("descricao" in payload):
                descricao = payload["descricao"]
            else:
                descricao = "-"

            query = """ INSERT INTO 
                        produto(id, marca, stock, vendedor_pessoa_id, descricao, preco, data, produto_num_versao)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, TIMESTAMP, %s)"""
            values = (id, marca, stock, vendedor_id, descricao, preco, 1)


            cur.execute("BEGIN TRANSACTION")
            cur.execute(query, values)
            cur.execute("COMMIT")


            if("processador" in payload and "sistema_operativo" in payload and "armazenamento" in payload and "camara" in payload):

                processador = payload["processador"]
                sistema_operativo = payload["sistema_operativo"]
                armazenamento = payload["armazenamento"]
                camara = payload["camara"]

                query2 = """ INSERT INTO
                            computador(processador, sistema_operativo, armazenamento, camara, produto_id, produto_num_versao)
                            VALUES
                            (%s, %s, %s, %s, %s)"""
                values2 = (processador, sistema_operativo, armazenamento, camara, id, 1)

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
                            televisor(comprimento, largura, peso, resolucao, produto_id, produto_num_versao)
                            VALUES
                            (%s, %s, %s, %s, %s, %s)"""
                values2 = (comprimento, largura, peso, resolucao, id, 1)

                cur.execute("BEGIN TRANSACTION")
                cur.execute(query2, values2)
                cur.execute("COMMIT")

                content = {'status': StatusCodes['success'], 'results': id}

            elif("modelo" in payload and "cor" in payload):

                modelo = payload["modelo"]
                cor = payload["cor"]

                query2 = """INSERT INTO
                            smartphone(modelo, cor, produto_id, produto_num_versao)
                            VALUES
                            (%s, %s, %s, %s)"""
                values2 = (modelo, cor, id, 1)

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

#TODO adapt to also showcase the stuff from the subclasses
#GET
@app.route('/dbproj/produto/<produto_id>', methods=['GET'])
def detalhes_produto(produto_id: str):
    #logger.info(f"Detalhes e histórico para produto com id {produto_id}")
    if not produto_id.isdigit():
        #logger
        return jsonify({'error' : 'Invalid product Id was provided', 'code': StatusCodes['api_error']})

    conn = db_connection()
    cur = conn.cursor()

    try:

        find_if_product = """   SELECT id
                                FROM produto
                                WHERE id = %s"""
        cur.execute(find_if_product, [produto_id])
        rows = cur.fetchall()
        if(rows.len > 0):   #produto existe

            #Do query to find if any of the subtables have a product with provided Id, if yes, do query also using data from that table
            find_comp_stmt =    """     SELECT produto_id
                                        FROM computador
                                        WHERE produto_id = %s;
                                """

            cur.execute(find_comp_stmt, [produto_id])
            rows = cur.fetchall()

            if(rows.len() > 0):
                comp_details_stmt = """  SELECT produto.titulo "Titulo", produto.stock "Stock", produto.preco "Preço", produto.num_versao "Versão", produto.marca "Marca", computador.processador "Processador", computador.sistema_operativo "SO", computador.armazenamento "Armazenamento", computador.camara "Câmara", produto.descricao "Descricao", produto.data "Data alteração"
                                            FROM produto, computador
                                            WHERE id = %s AND produto.id = computador.produto_id
                                            ORDER BY data ASC;"""

                cur.execute("BEGIN TRANSACTION")
                cur.execute(comp_details_stmt, [produto_id])
                cur.execute("COMMIT")

                rows = cur.fetchall()

                if (rows.len() > 0):
                    content = {"code": StatusCodes['success']}
                else:
                    content = {"code": StatusCodes['api_error']}

            else:
                find_tel_stmt = """     SELECT produto_id
                                        FROM televisor
                                        WHERE produto_id = %s;"""

                cur.execute(find_tel_stmt, [produto_id])
                rows = cur.fetchall()
                if (rows.len() > 0):
                    tel_details_stmt = """  SELECT produto.titulo "Titulo", produto.stock "Stock", produto.preco "Preço", produto.num_versao "Versão", produto.marca "Marca", televisor.comprimento "Comprimento", televisor.largura "Largura", televisor.peso "Peso", televisor.resolucao "Resolução", produto.descricao "Descricao", produto.data "Data alteração"
                                            FROM produto, televisor
                                            WHERE id = %s AND produto.id = televisor.produto_id
                                            ORDER BY data ASC;"""

                    cur.execute("BEGIN TRANSACTION")
                    cur.execute(tel_details_stmt, [produto_id])
                    cur.execute("COMMIT")

                    rows = cur.fetchall()

                    if (rows.len() > 0):
                        content = {"code": StatusCodes['success']}
                    else:
                        content = {"code": StatusCodes['api_error']}

                else:
                    find_phone_stmt = """   SELECT produto_id
                                            FROM smartphone
                                            WHERE produto_id = %s;"""

                    cur.execute(find_phone_stmt, [produto_id])
                    rows = cur.fetchall()
                    if (rows.len() > 0):
                        phone_details_stmt = """  SELECT produto.titulo "Titulo", produto.stock "Stock", produto.preco "Preço", produto.num_versao "Versão", produto.marca "Marca", smartphone.modelo "Modelo", smartphone.cor "Cor", produto.descricao "Descricao", produto.data "Data alteração"
                                                FROM produto, televisor
                                                WHERE id = %s AND produto.id = televisor.produto_id
                                                ORDER BY data ASC;"""

                        cur.execute("BEGIN TRANSACTION")
                        cur.execute(phone_details_stmt, [produto_id])
                        cur.execute("COMMIT")

                        rows = cur.fetchall()

                        if (rows.len() > 0):
                            content = {"code": StatusCodes['success']}
                        else:
                            content = {"code": StatusCodes['api_error']}

                    else:
                        #if not a specific type of product, prints general table
                        produto_details_stmt = """  SELECT titulo "Titulo", stock "Stock", marca "Marca", preco "Preço", num_versao "Versão", descricao "Descricao", data "Data alteração"
                                                    FROM produto
                                                    WHERE id = %s 
                                                    ORDER BY data ASC;"""

                        cur.execute("BEGIN TRANSACTION")
                        cur.execute(produto_details_stmt, [produto_id])
                        cur.execute("COMMIT")

                        rows = cur.fetchall()

                        if (rows.len() > 0):
                            content = {"code": StatusCodes['success']}
                        else:
                            content = {"code": StatusCodes['api_error']}

        else:
            #TODO error message for product not found
            print("no error")


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