from myairflow.send_notification import send_noti

def test_notification():
    msg = "pytest: nunininu"
    r = send_noti(msg)
    assert r == 204


