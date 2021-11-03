import datetime
import time
import typing

import app.entities as entities


def _compose_text(event: entities.EmailEvent, utc_ts: str, timestamp: str):
    return f"""{event.warning}\n
     - PV name:         {event.pvname}
     - Specified range: {event.specified_value_message}
     - Value measured:  {event.value_measured} {event.unit}
     - Timestamp:       {timestamp}

     Archiver link:
        https://10.0.38.42/archiver-vewer/?pv={event.pvname}&to={utc_ts}
        https://10.0.38.46/archiver-vewer/?pv={event.pvname}&to={utc_ts}
        https://10.0.38.59/archiver-vewer/?pv={event.pvname}&to={utc_ts}

     Controls Group\n"""


def _compose_html(event: entities.EmailEvent, utc_ts: str, timestamp: str):
    return f"""\
        <html>
            <body>
                <p> {event.warning} <br>
                    <ul>
                        <li><b>PV name:         </b> {event.pvname} <br></li>
                        <li><b>Specified range: </b> {event.specified_value_message}<br></li>
                        <li><b>Value measured:  </b> {event.value_measured} {event.unit}<br></li>
                        <li><b>Timestamp:       </b> {timestamp}<br></li>
                    </ul>
                    Archiver link:
                       <a href="https://10.0.38.42/archiver-vewer/?pv={event.pvname}&to={utc_ts}">https://10.0.38.42/archiver-vewer/?pv={event.pvname}&to={utc_ts}</a><br><br>
                       <a href="https://10.0.38.46/archiver-vewer/?pv={event.pvname}&to={utc_ts}">https://10.0.38.46/archiver-vewer/?pv={event.pvname}&to={utc_ts}</a><br><br>
                       <a href="https://10.0.38.59/archiver-vewer/?pv={event.pvname}&to={utc_ts}">https://10.0.38.59/archiver-vewer/?pv={event.pvname}&to={utc_ts}</a><br><br>

                    GAS - Automação e Software
                </p>
            </body>
        </html>"""


def compose_msg_content(event: entities.EmailEvent) -> typing.Tuple[str, str]:
    """
    @return (text, html)
    """
    timestamp = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    utc_ts = "{}Z".format(
        datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    )

    # creating the plain-text format of the message
    text = _compose_text(event, utc_ts, timestamp)

    html = _compose_html(event, utc_ts, timestamp)
    return text, html
