local terminal = require("terminal")
local utils = require("utils")

utils.map('n', '<f9>', function()
    terminal.launch_terminal('ssh viraat_chandra_24@hpc.brc.berkeley.edu', false, terminal.set_target)
end)
