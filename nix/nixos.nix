# NixOS module: services.evakuilo — Wikidot evacuation archiver daemon.
#
# Usage:
#   inputs.evakuilo.url = "github:<owner>/wikidot-evakuilo";
#   ...
#   imports = [ inputs.evakuilo.nixosModules.default ];
#
#   services.evakuilo = {
#     enable = true;
#     settings.instance = {
#       name = "kolorinko";
#       sites = [ "obscurative" "rpcauthority" "wci" ];
#     };
#   };
#
# The daemon is self-sufficient on start: it opens (creating if needed) the
# per-site SQLite databases under {settings.data_dir}/{instance} and seeds
# the periodic job queue itself, so there is no separate init step.
self:
{ config, lib, pkgs, ... }:

let
  cfg = config.services.evakuilo;
  format = pkgs.formats.toml { };
in
{
  options.services.evakuilo = {
    enable = lib.mkEnableOption "evakuilo, the Wikidot evacuation archiver daemon";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = "evakuilo from the evakuilo flake";
      description = "The evakuilo package to run.";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "evakuilo";
      description = "User to run the daemon as.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "evakuilo";
      description = "Group to run the daemon as.";
    };

    configFile = lib.mkOption {
      type = lib.types.nullOr lib.types.path;
      default = null;
      description = ''
        Path to a hand-written `evakuilo.toml` (one instance per file).
        When set, it replaces the config generated from
        {option}`services.evakuilo.settings`.
      '';
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        freeformType = format.type;
        options = {
          data_dir = lib.mkOption {
            type = lib.types.path;
            default = "/var/lib/evakuilo";
            description = ''
              Root data directory; the instance lives at
              `{data_dir}/{instance.name}` (`meta/` is private state,
              `out/` is the published tree). Only the root is created by
              the module — the daemon creates the rest.
            '';
          };
          instance = lib.mkOption {
            type = lib.types.submodule {
              freeformType = format.type;
              options = {
                name = lib.mkOption {
                  type = lib.types.str;
                  default = "evakuilo";
                  description = "Instance name (directory name under data_dir).";
                };
                sites = lib.mkOption {
                  type = lib.types.listOf lib.types.str;
                  default = [ ];
                  description = "Wikidot site slugs to archive.";
                };
              };
            };
            default = { };
            description = "The `[instance]` table: one daemon, one instance.";
          };
        };
      };
      default = { };
      example = {
        data_dir = "/var/lib/evakuilo";
        instance = {
          name = "kolorinko";
          sites = [ "obscurative" "rpcauthority" "wci" ];
        };
        rate_limit_ms = 2000;
        zstd_level = 19;
      };
      description = ''
        Contents of `evakuilo.toml`, generated unless
        {option}`configFile` is set. Free-form: every key of the config
        file (timeout_s, rate_limit_ms, monitor_interval_s, shell_interval_s,
        out_interval_s, backfill_interval_s, zstd_level, …) is accepted.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.settings.instance.sites != [ ];
        message = "services.evakuilo.settings.instance.sites must list at least one site";
      }
    ];

    users.users.evakuilo = lib.mkIf (cfg.user == "evakuilo") {
      isSystemUser = true;
      group = cfg.group;
      description = "evakuilo daemon user";
    };
    users.groups.evakuilo = lib.mkIf (cfg.group == "evakuilo") { };

    systemd.tmpfiles.rules = [
      "d ${cfg.settings.data_dir} 0700 ${cfg.user} ${cfg.group} - -"
    ];

    systemd.services.evakuilo = {
      description = "evakuilo — Wikidot evacuation archiver";
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      wantedBy = [ "multi-user.target" ];

      environment.EVAKUILO_CONFIG =
        if cfg.configFile != null then
          cfg.configFile
        else
          format.generate "evakuilo.toml" cfg.settings;

      serviceConfig = {
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${lib.getExe cfg.package} run";
        WorkingDirectory = cfg.settings.data_dir;
        Restart = "on-failure";
        RestartSec = "10s";

        # Hardening: the daemon only needs network + its data dir.
        NoNewPrivileges = true;
        PrivateTmp = true;
        PrivateDevices = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProtectSystem = "strict";
        ReadWritePaths = [ cfg.settings.data_dir ];
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        SystemCallArchitectures = "native";
        SystemCallFilter = [ "@system-service" ];
        UMask = "0077";
      };
    };
  };
}
