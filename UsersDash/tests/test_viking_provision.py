"""Интеграционная проверка подтверждений Viking Tools на изолированной БД."""

import unittest
from unittest.mock import patch

from flask import Flask

from UsersDash.api_views import api_bp
from UsersDash.models import Account, FarmData, Server, db


class VikingProvisionTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config.update(SQLALCHEMY_DATABASE_URI='sqlite://', TESTING=True)
        db.init_app(self.app)
        self.app.register_blueprint(api_bp, url_prefix='/api')
        self.context = self.app.app_context()
        self.context.push()
        db.create_all()
        db.session.add_all([
            Server(name='208', host='127.0.0.1', api_token='token', is_active=True),
            Server(name='F99', host='127.0.0.2', api_token='other', is_active=True),
        ])
        db.session.commit()
        self.client = self.app.test_client()
        self.row = dict(internal_id='a' * 32, name='TestNew1', email='one@example.test',
                        password='secret', igg_id='12345', tariff=500)

    def tearDown(self):
        db.session.remove()
        db.drop_all()
        self.context.pop()

    def post(self, endpoint, payload, server='208', token='token'):
        return self.client.post('/api/farms/v1/provision/' + endpoint, query_string={'server': server},
                                headers={'Authorization': 'Bearer ' + token}, json=payload)

    def test_reserve_is_idempotent_and_complete_persists_data(self):
        from UsersDash.models import User
        owner = User(username='test-owner', password_hash='unused', role='client')
        db.session.add(owner)
        db.session.commit()
        with patch('UsersDash.admin_views._get_or_create_client_for_farm', return_value=owner):
            first = self.post('reserve', self.row)
            second = self.post('reserve', self.row)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json['account_id'], second.json['account_id'])
        self.assertEqual(Account.query.count(), 1)
        self.assertFalse(Account.query.one().is_active)
        response = self.post('complete', dict(internal_id='a' * 32, name='TestNew1', instance_id=9, igg_id='1234567890'))
        self.assertEqual(response.status_code, 200)
        self.assertTrue(Account.query.one().is_active)
        self.assertEqual(FarmData.query.one().igg_id, '1234567890')
        self.assertEqual(Account.query.one().next_payment_tariff, 500)
        self.assertNotIn('secret', str(response.json))

    def test_server_token_cannot_complete_another_servers_operation(self):
        from UsersDash.models import User
        owner = User(username='test-owner', password_hash='unused', role='client')
        db.session.add(owner)
        db.session.commit()
        with patch('UsersDash.admin_views._get_or_create_client_for_farm', return_value=owner):
            self.post('reserve', self.row)
        response = self.post('complete', dict(internal_id='a' * 32, name='TestNew1', instance_id=9, igg_id='1234567890'),
                             server='F99', token='other')
        self.assertEqual(response.status_code, 409)
        self.assertFalse(Account.query.one().is_active)

    def test_bad_token_and_wrong_game_id_do_not_modify_account(self):
        self.assertEqual(self.post('reserve', self.row, token='bad').status_code, 400)
        from UsersDash.models import User
        owner = User(username='test-owner', password_hash='unused', role='client')
        db.session.add(owner)
        db.session.commit()
        with patch('UsersDash.admin_views._get_or_create_client_for_farm', return_value=owner):
            self.post('reserve', self.row)
        response = self.post('complete', dict(internal_id='a' * 32, name='TestNew1', instance_id=9, igg_id='9999912345'))
        self.assertEqual(response.status_code, 409)
        self.assertFalse(Account.query.one().is_active)
        self.assertEqual(FarmData.query.one().igg_id, '12345')


if __name__ == '__main__':
    unittest.main()
