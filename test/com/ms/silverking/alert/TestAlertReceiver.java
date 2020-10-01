package com.ms.silverking.alert;

public class TestAlertReceiver implements AlertReceiver {

  private Alert alert;

  public TestAlertReceiver() {
    alert = null;
  }

  @Override
  public void sendAlert(Alert alert) {
    this.alert = alert;
  }

  String getAlert() {
    if (alert == null)
      return "";

    return alert.toString();
  }
}
