from xflow.framework.pipeline import Pipeline


class example(Pipeline):
    """
    pipeline 示例。
    """
    class Options(Pipeline.Options):
        """
        流水线参数，根据需要自行定义。
        """
        pyver: int = Pipeline.Option(desc='Python version.',
                                     default=3)
        packtype: str = Pipeline.Option(desc='Package type.',
                                        default='onedir',
                                        choices=('onefile', 'onedir'))
    
    def setup(self) -> None:
        """
        前置步骤。
        """
        self.options: __class__.Options  # 用于类型推断
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
        with self.node.dir('xbot.framework'):
            self.node.exec(f'python{self.options.pyver} -m venv venv')
            self.node.exec('venv/bin/pip install -r requirements.txt')
            self.node.exec('venv/bin/pip install pyinstaller')
            self.node.exec(f'venv/bin/python -m PyInstaller --{self.options.packtype} -n xbot xbot/framework/main.py')

    def stage3(self) -> None:
        """
        打包。
        """
        self.node.putfile(
            './requirements.txt',
            self.node.cwd.joinpath('xbot.framework', 'dist')
        )
        with self.node.dir('xbot.framework/dist'):
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
