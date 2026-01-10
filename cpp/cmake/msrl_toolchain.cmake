function(msrl_apply_warnings tgt)
  if (MSVC)
    target_compile_options(${tgt} PRIVATE /W4 /permissive- /EHsc)
  else()
    target_compile_options(${tgt} PRIVATE -Wall -Wextra -Wpedantic)
  endif()
endfunction()

function(msrl_apply_opt tgt)
  if (MSVC)
    target_compile_options(${tgt} PRIVATE
      $<$<CONFIG:Release>:/O2>
      $<$<CONFIG:RelWithDebInfo>:/O2>
      $<$<CONFIG:MinSizeRel>:/O1>
    )
  endif()
endfunction()
