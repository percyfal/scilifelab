"""Delivery module"""

def transfer_files(sources, targets):
    """Transfer source files to target locations"""
    for src, tgt in zip(sources['files'] + sources['results'], targets['files'] + targets['results']):
        if not os.path.exists(os.path.dirname(tgt)):
            self.app.cmd.safe_makedir(os.path.dirname(tgt))
        self.app.cmd.transfer_file(src, tgt)

