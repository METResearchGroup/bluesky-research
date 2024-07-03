"""Mock users to add to the study."""

# raw SQL data, from previous database implementation
raw_sql_data = """
did:plc:w5mjarupsl6ihdrzwgnzdh4y|markptorres.bsky.social|reverse_chronological
did:plc:e4itbqoxctxwrrfqgs2rauga|williambrady.bsky.social|engagement
did:plc:vhqugnjlbetilpna3xqe24ks|steveread.bsky.social|representative_diversification
did:plc:imkshv4k5vdpapapyxaqkvwv|lukethorburn.com|reverse_chronological
did:plc:j4d6x5nng6qdn3lwrikz7z6t|adriennesolange.bsky.social|engagement
did:plc:nnpkrdmihaxs62evxm2rkumz|bensaun.bsky.social|representative_diversification
did:plc:356vkcyse2cqkh5rnzkv2xbc|maxhuibai.bsky.social|reverse_chronological
did:plc:yl7wcldipsfnjdww2jg5mnrv|teonbrooks.com|engagement
did:plc:vnfqcj4yngpqyqszi2g352pn|evgoldfarb.bsky.social|representative_diversification
did:plc:5d4dkeqbjuo4iv5fjxwzfoze|larakirfel.bsky.social|reverse_chronological
did:plc:v337yqdweugxve7toqgix5bz|kcoe.bsky.social|engagement
did:plc:g6to7fuhvms3jyh5hyywrvbg|yotam.bsky.social|representative_diversification
did:plc:fua4kdthqiuzvzrkerinpygv|davidchester.bsky.social|reverse_chronological
did:plc:mvvopd2jj3432twfga7nvpcm|simonkarg.bsky.social|engagement
did:plc:qqezpa6tha2kc6fcvad5yzbl|urihertz.bsky.social|representative_diversification
did:plc:mgeebc7qyhgiqn7skqkaamrh|joebayer.bsky.social|reverse_chronological
did:plc:4minmxuahmuiubboj46kgptd|iyadrahwan.bsky.social|engagement
did:plc:whrqgoxrbuilgsmokfpz7hdx|jaeyoungson.bsky.social|representative_diversification
did:plc:ecg6xi5wymsghbfhgh6aw7ga|lornejcampbell.bsky.social|reverse_chronological
did:plc:756aszd5o4vupbfi6epcpkwn|mrossignacmilon.bsky.social|engagement
did:plc:4iiabj43ufejwi7gafwufnjz|robbwiller.bsky.social|representative_diversification
did:plc:peasj7yvw2smb3z7t7y24ao3|vaparker.bsky.social|reverse_chronological
did:plc:lcvue6syxlezwxvimilp3a6e|elijfinkel.bsky.social|engagement
did:plc:h7gcpcvlfqijzr7evavuqkvs|mikeberkwein.bsky.social|representative_diversification
did:plc:wzdseyhopxdhepo3li4lbzxn|katharinalawall.bsky.social|reverse_chronological
did:plc:tiktzgm7ytptb3u6sgn3psy2|awiezel.bsky.social|representative_diversification
did:plc:tjbpg4fabbtgwgtkdwtzmax3|kmarkman.bsky.social|reverse_chronological
did:plc:wvb6v45g6oxrfebnlzllhrpv|duniganf.bsky.social|engagement
did:plc:fbnm4hjnzu4qwg3nfjfkdhay|hannesrusch.bsky.social|representative_diversification
did:plc:iriqg3r7ztl6rxsryw4khz7v|dcameron.bsky.social|engagement
"""
lines = raw_sql_data.strip().split('\n')
mock_users: list[dict] = []

for line in lines:
    did, handle, condition = line.split('|')
    user = {
        "bluesky_user_did": did,
        "bluesky_handle": handle,
        "condition": condition
    }
    mock_users.append(user)
