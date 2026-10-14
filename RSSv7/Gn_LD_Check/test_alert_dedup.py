import unittest
import emulator_health as h

def sample(failed=()):
    return dict(ready=[1],windows=[1]+list(failed),headless=[1]+list(failed),
        failed={str(k):'offline' for k in failed},errors=[],gn_running=True)

class NotificationTests(unittest.TestCase):
    def confirmed(self,failed,state,at):
        state,_,_,_=h.evaluate(sample(failed),state,at)
        return h.evaluate(sample(failed),state,at+180)
    def sent(self,failed,at=1000):
        state,send,sig,_=self.confirmed(failed,{},at)
        self.assertTrue(send);h.mark_delivered(state,sig,at+180)
        return state
    def test_window_disappearing_does_not_reset_delivery_limit(self):
        state=self.sent([7])
        state,_,_,_=h.evaluate(sample(),state,1240)
        _,send,_,_=self.confirmed([7],state,1300)
        self.assertFalse(send)
    def test_changed_failure_combination_does_not_resend_existing_target(self):
        state=self.sent([7])
        state,send,sig,confirmed=self.confirmed([7,9],state,1240)
        self.assertTrue(send);self.assertEqual(sig,'9');self.assertEqual(confirmed,['9'])
        h.mark_delivered(state,sig,1420)
        _,send,_,_=h.evaluate(sample([7]),state,1480)
        self.assertFalse(send)
    def test_repeat_is_allowed_only_after_30_minutes_per_target(self):
        state=self.sent([7])
        _,send,_,_=h.evaluate(sample([7]),state,2979)
        self.assertFalse(send)
        _,send,_,_=h.evaluate(sample([7]),state,2980)
        self.assertTrue(send)
    def test_new_failure_is_not_hidden_by_other_target_cooldown(self):
        state=self.sent([7])
        state,_,_,_=h.evaluate(sample(),state,1240)
        _,send,sig,_=self.confirmed([52],state,1300)
        self.assertTrue(send);self.assertEqual(sig,'52')
    def test_mass_outage_escalation_is_not_suppressed(self):
        state=self.sent([7]);s=sample([7]);s['ready']=[]
        state,_,_,_=h.evaluate(s,state,1240)
        state,send,sig,_=h.evaluate(s,state,1420)
        self.assertTrue(send);self.assertEqual(sig,'all')
        h.mark_delivered(state,sig,1420)
        _,send,_,_=h.evaluate(sample([7]),state,1480)
        self.assertFalse(send)
    def test_legacy_sent_state_migrates(self):
        prev=dict(last_signature='7',last_sent=1180,failed_since={'7':1000})
        state,send,_,_=h.evaluate(sample([7]),prev,1240)
        self.assertFalse(send);self.assertEqual(state['sent_by_key']['vm:7'],1180)
    def test_no_delivery_ack_does_not_consume_cooldown(self):
        state,send,sig,_=self.confirmed([7],{},1000)
        self.assertTrue(send);self.assertNotIn('vm:7',state['sent_by_key'])
        _,send,_,_=h.evaluate(sample([7]),state,1240)
        self.assertTrue(send)

if __name__=='__main__':unittest.main()
