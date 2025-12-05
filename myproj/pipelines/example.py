from xflow.framework.pipeline import Pipeline


class example(Pipeline):
    """
    pipeline 示例。
    """
    def setup(self) -> None:
        """
        前置步骤。
        """
        super().setup()

    def stage1(self) -> None:
        """
        拉取代码。
        """
        self.node.exec('git clone https://ghfast.top/https://github.com/zhaowcheng/xbot.framework.git')

    def stage2(self) -> None:
        """
        编译代码。
        """
        with self.node.cd('xbot.framework'):
            self.node.exec('python3 -m venv venv')
            self.node.exec('venv/bin/pip install -r requirements.txt')
            self.node.exec('venv/bin/pip install pyinstaller')
            self.node.exec('venv/bin/python -m PyInstaller --onedir -n xbot xbot/framework/main.py')

    def stage3(self) -> None:
        """
        打包。
        """
        self.node.putfile(
            './requirements.txt',
            self.node.cwd.joinpath('xbot.framework', 'dist', 'xbot')
        )
        with self.node.cd('xbot.framework/dist'):
            self.node.exec('tar czvf xbot.tar.gz xbot/')
        self.node.getfile(
            self.node.cwd.joinpath('xbot.framework', 'dist', 'xbot.tar.gz'),
            self.cwd
        )

    def teardown(self) -> None:
        """
        后置步骤。
        """
        super().teardown()
