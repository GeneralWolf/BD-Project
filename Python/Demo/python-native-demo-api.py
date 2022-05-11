##
## =============================================
## ============== Bases de Dados ===============
## ============== LEI  2021/2022 ===============
## =============================================
## =================== Demo ====================
## =============================================
## =============================================
## === Department of Informatics Engineering ===
## =========== University of Coimbra ===========
## =============================================
##
## Authors:
##   Nuno Antunes <nmsa@dei.uc.pt>
##   BD 2022 Team - https://dei.uc.pt/lei/
##   University of Coimbra


import flask
import logging
import psycopg2
import time

app = flask.Flask(__name__)

StatusCodes = {
    'success': 200,
    'api_error': 400,
    'internal_error': 500
}

##########################################################
## DATABASE ACCESS
##########################################################

def db_connection():
    db = psycopg2.connect(
        user='aulaspl',
        password='aulaspl',
        host='127.0.0.1',
        port='5432',
        database='dbfichas'
    )

    return db




##########################################################
## ENDPOINTS
##########################################################

#TODO remove old examples later
"""
@app.route('/') 
def landing_page():
    return """
"""
    Hello World (Python Native)!  <br/>
    <br/>
    Check the sources for instructions on how to use the endpoints!<br/>
    <br/>
    BD 2022 Team<br/>
    <br/>
    """
"""

##
## Demo GET
##
## Obtain all departments in JSON format
##
## To use it, access: 
## 
## http://localhost:8080/departments/
##

@app.route('/departments/', methods=['GET'])
def get_all_departments():
    logger.info('GET /departments')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT ndep, nome, local FROM dep')
        rows = cur.fetchall()

        logger.debug('GET /departments - parse')
        Results = []
        for row in rows:
            logger.debug(row)
            content = {'ndep': int(row[0]), 'nome': row[1], 'localidade': row[2]}
            Results.append(content)  # appending to the payload to be returned

        response = {'status': StatusCodes['success'], 'results': Results}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /departments - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
## Demo GET
##
## Obtain department with ndep <ndep>
##
## To use it, access: 
## 
## http://localhost:8080/departments/10
##

@app.route('/departments/<ndep>/', methods=['GET'])
def get_department(ndep):
    logger.info('GET /departments/<ndep>')

    logger.debug(f'ndep: {ndep}')

    conn = db_connection()
    cur = conn.cursor()

    try:
        cur.execute('SELECT ndep, nome, local FROM dep where ndep = %s', (ndep,))
        rows = cur.fetchall()

        row = rows[0]

        logger.debug('GET /departments/<ndep> - parse')
        logger.debug(row)
        content = {'ndep': int(row[0]), 'nome': row[1], 'localidade': row[2]}

        response = {'status': StatusCodes['success'], 'results': content}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'GET /departments/<ndep> - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)


##
## Demo POST
##
## Add a new department in a JSON payload
##
## To use it, you need to use postman or curl: 
##
## curl -X POST http://localhost:8080/departments/ -H 'Content-Type: application/json' -d '{'localidade': 'Polo II', 'ndep': 69, 'nome': 'Seguranca'}'
##

@app.route('/departments/', methods=['POST'])
def add_departments():
    logger.info('POST /departments')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'POST /departments - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'ndep' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'ndep value not in payload'}
        return flask.jsonify(response)

    # parameterized queries, good for security and performance
    statement = 'INSERT INTO dep (ndep, nome, local) VALUES (%s, %s, %s)'
    values = (payload['ndep'], payload['localidade'], payload['nome'])

    try:
        cur.execute(statement, values)

        # commit the transaction
        conn.commit()
        response = {'status': StatusCodes['success'], 'results': f'Inserted dep {payload["ndep"]}'}

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(f'POST /departments - error: {error}')
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)

##
## Demo PUT
##
## Update a department based on a JSON payload
##
## To use it, you need to use postman or curl: 
##
## curl -X PUT http://localhost:8080/departments/ -H 'Content-Type: application/json' -d '{'ndep': 69, 'localidade': 'Porto'}'
##

@app.route('/departments/<ndep>', methods=['PUT'])
def update_departments(ndep):
    logger.info('PUT /departments/<ndep>')
    payload = flask.request.get_json()

    conn = db_connection()
    cur = conn.cursor()

    logger.debug(f'PUT /departments/<ndep> - payload: {payload}')

    # do not forget to validate every argument, e.g.,:
    if 'localidade' not in payload:
        response = {'status': StatusCodes['api_error'], 'results': 'localidade is required to update'}
        return flask.jsonify(response)

    # parameterized queries, good for security and performance
    statement = 'UPDATE dep SET local = %s WHERE ndep = %s'
    values = (payload['localidade'], ndep)

    try:
        res = cur.execute(statement, values)
        response = {'status': StatusCodes['success'], 'results': f'Updated: {cur.rowcount}'}

        # commit the transaction
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
        response = {'status': StatusCodes['internal_error'], 'errors': str(error)}

        # an error occurred, rollback
        conn.rollback()

    finally:
        if conn is not None:
            conn.close()

    return flask.jsonify(response)
"""

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

if __name__ == '__main__':
    
    # set up logging
    logging.basicConfig(filename='log_file.log')
    logger = logging.getLogger('logger')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s [%(levelname)s]:  %(message)s', '%H:%M:%S')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    host = '127.0.0.1'
    port = 8080
    app.run(host=host, debug=True, threaded=True, port=port)
    logger.info(f'API v1.1 online: http://{host}:{port}')
