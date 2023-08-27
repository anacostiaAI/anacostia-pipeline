from typing import Dict

import pkg_resources
from rich.prompt import Prompt, Confirm


class Shell:
    def __init__(self, target_pipline):
        self.pipeline = target_pipline
        self.console = self.pipeline.console

    def motd(self):
        msg = \
r'''    _                                     _    _          ____   _               _  _              
   / \    _ __    __ _   ___   ___   ___ | |_ (_)  __ _  |  _ \ (_) _ __    ___ | |(_) _ __    ___ 
  / _ \  | '_ \  / _` | / __| / _ \ / __|| __|| | / _` | | |_) || || '_ \  / _ \| || || '_ \  / _ \
 / ___ \ | | | || (_| || (__ | (_) |\__ \| |_ | || (_| | |  __/ | || |_) ||  __/| || || | | ||  __/
/_/   \_\|_| |_| \__,_| \___| \___/ |___/ \__||_| \__,_| |_|    |_|| .__/  \___||_||_||_| |_| \___|
                                                                   |_|                             
'''
        self.console.print(msg)

    def help_cmd(self, cmd):
        repo_url = "<Website Name Here>"

        common_commands = {
            "help": "Displays this help text",
            "version": "Prints the anacostia-pipeline module version number",
            "save": "Serialize the Pipeline to a Json File"
        }

        management_commands = {
            "pipe": "Manage the Pipeline",
            "node": "Manage Individual Nodes",
        }

        help_text = "\nA Machine Learning DevOps Pipeline\n\n" + \
                    self._sub_command_help("Common Commands", common_commands) + \
                    self._sub_command_help("Management Commands", management_commands) + \
                    "Run \'COMMAND --help\' for more information on a command\n" + \
                    f"For more information see {repo_url}\n"

        self.console.print(help_text)

    def _sub_command_help(self, title:str, commands:Dict[str, str]):
        
        longest = max([len(c) for c in commands.keys()]) + 4

        return f"{title}:\n" + \
            "\n".join(" {}{}{}".format(cmd, " "*(longest-len(cmd)), txt) for cmd, txt in commands.items()) + "\n\n"
        

    def _usage(cmd:str, options:str, description:str, options:List[Tuple], suffx:str=None):
        max_line_len = 80
        usage_txt = f"Usage: {cmd} {options}\n{description}\n\n"

        longest = max([len(o[1]) for o in options])

        for option in options:
            short, _long, descript = option
            txt = "  "
            if short:
                txt += f"-{short}"
            else:
                txt += "  "

            if _long:
                txt += ", --{_long}"
            else:
                _long = ""
                txt += "    "

            txt += " "*(longest - len(_long) + 6) + descript
            usage_txt += txt
        
        if suffix:
            usage_txt += f"\n\n{suffix}"

    def pipe_cmds(self, cmd):
        command = cmd[0]
        cmd = [1:]

        match command:
            case "stop":
                pass
            case "pause":
                pass
            case "status":
                pass

    def node_cmds(self, cmd):
        pass

    def save(self, cmd):
        pass

    def start(self):
        self.motd()

        try:
            while True:        
                cmd_string = self.console.input("> ")
                cmd = [c for c in cmd_string.strip().split() if len(c.strip()) > 0]

                command = cmd[0] 
                cmd = cmd[1:]

                match command:
                    case "help":
                        self.help_cmd(cmd)
                    case "version":
                        version = pkg_resources.get_distribution("anacostia-pipeline").version
                        self.console.print(f"Version {version}")
                    case "pipe":
                        self.pipe_cmds(cmd)
                    case "node":
                        self.node_cmds(cmd)
                    case "save":
                        self.save(cmd)

        except KeyboardInterrupt:
            self.console.print("Ctrl+C Detected")
            self.pipeline.pause_nodes()
            answer = Prompt.ask("Are you sure you want to shutdown the pipeline?", console=self.console, default='n')

            if answer == 'y':

                answer = Prompt.ask(
                    "Do you want to do a hard shutdown, soft shutdown, or abort the shutdown?", 
                    console=self.console, default='abort',
                    choices=['hard', 'soft', 'abort']
                )
                
                if answer == 'hard':
                    self.console.print("Hard Shutdown")
                    self.pipeline.terminate_nodes()
                    exit(1)

                elif answer == 'soft':
                    print("Shutting down pipeline")
                    self.pipeline.terminate_nodes() 
                    for node in reversed(self.pipeline.nodes):
                        node.teardown()
                    print("Pipeline shutdown complete")
                    exit(0)

                else:
                    print("Aborting shutdown, resuming pipeline")
                    for node in self.pipeline.nodes:
                        node.resume()
            
            else:
                print("Aborting shutdown, resuming pipeline")
                for node in self.pipeline.nodes:
                    node.resume()