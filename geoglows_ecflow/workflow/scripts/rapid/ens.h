%includeonce <suite.h>
%includeonce <scripts.h>
%includeonce <comfies.h>

ens_ymd=%YMD:none%

if [[ %DELTA_DAY:0% -ne 0 ]]; then
    ens_ymd=$(calseq shift $ens_ymd %DELTA_DAY:0%)
fi

ens_base=%EMOS_BASE%
ens_basetime=$ens_ymd$ens_base


ens_pymd=$(calseq shift $ens_ymd -1)
ens_pymd1=$(calseq shift $ens_ymd -2)
ens_nymd=$(calseq shift $ens_ymd +1)

#day of week (1:Monday, 2:Tuesday etc)
ens_dow_num=$(date -d $ens_ymd +'%%u')
ens_dow=$(date -d $ens_ymd +'%%a')

ens_year=${ens_ymd:0:4}
ens_month=${ens_ymd:4:2}
ens_day=${ens_ymd:6:2}

ens_dmy="$ens_day/$ens_month/$ens_year"

ens_pyear=${ens_pymd:0:4}
ens_pmonth=${ens_pymd:4:2}
ens_pday=${ens_pymd:6:2}

ens_workdir=$suite_workdir
ens_pworkdir=$suite_workdir

ens_inputdir=$ens_workdir/grib/$ens_basetime

ens_fcdir=$ens_workdir/fc/$ens_basetime
ens_rapid_input=$ens_fcdir/input
ens_rapid_output=$ens_fcdir/output

ens_member=%MEMBER:0%
ens_nmembers=%MEMBERS:51%
ens_members=$(seq -f '%%02g' 0 $((ens_nmembers -1 )))
ens_mars_members=$(strjoin / $ens_members)
ens_mars_expver='expver=<?config.get('forecast_forcings_expver', '0001')?>'

